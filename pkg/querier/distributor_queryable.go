package querier

import (
	"context"
	"github.com/cortexproject/cortex/pkg/storage/exemplars"
	"github.com/cortexproject/cortex/pkg/tenant"
	"sort"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/math"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error)
	QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error)
	QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error)
	LabelValuesForLabelName(ctx context.Context, from, to model.Time, label model.LabelName, matchers ...*labels.Matcher) ([]string, error)
	LabelValuesForLabelNameStream(ctx context.Context, from, to model.Time, label model.LabelName, matchers ...*labels.Matcher) ([]string, error)
	LabelNames(context.Context, model.Time, model.Time) ([]string, error)
	LabelNamesStream(context.Context, model.Time, model.Time) ([]string, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error)
	MetricsForLabelMatchersStream(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error)
	MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error)
}

func newDistributorQueryable(distributor Distributor, streaming bool, streamingMetdata bool, iteratorFn chunkIteratorFunc, queryIngestersWithin time.Duration, queryStoreForLabels bool) QueryableWithFilter {
	return distributorQueryable{
		distributor:          distributor,
		streaming:            streaming,
		streamingMetdata:     streamingMetdata,
		iteratorFn:           iteratorFn,
		queryIngestersWithin: queryIngestersWithin,
		queryStoreForLabels:  queryStoreForLabels,
	}
}

type distributorQueryable struct {
	distributor          Distributor
	streaming            bool
	streamingMetdata     bool
	iteratorFn           chunkIteratorFunc
	queryIngestersWithin time.Duration
	queryStoreForLabels  bool
}

func (d distributorQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &distributorQuerier{
		distributor:          d.distributor,
		ctx:                  ctx,
		mint:                 mint,
		maxt:                 maxt,
		streaming:            d.streaming,
		streamingMetadata:    d.streamingMetdata,
		chunkIterFn:          d.iteratorFn,
		queryIngestersWithin: d.queryIngestersWithin,
		queryStoreForLabels:  d.queryStoreForLabels,
	}, nil
}

func (d distributorQueryable) UseQueryable(now time.Time, _, queryMaxT int64) bool {
	// Include ingester only if maxt is within QueryIngestersWithin w.r.t. current time.
	return d.queryIngestersWithin == 0 || queryMaxT >= util.TimeToMillis(now.Add(-d.queryIngestersWithin))
}

type distributorQuerier struct {
	distributor          Distributor
	ctx                  context.Context
	mint, maxt           int64
	streaming            bool
	streamingMetadata    bool
	chunkIterFn          chunkIteratorFunc
	queryIngestersWithin time.Duration
	queryStoreForLabels  bool
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *distributorQuerier) Select(sortSeries bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	log, ctx := spanlogger.New(q.ctx, "distributorQuerier.Select")
	defer log.Span.Finish()

	minT, maxT := q.mint, q.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	// If the querier receives a 'series' query, it means only metadata is needed.
	// For the specific case where queryStoreForLabels is disabled
	// we shouldn't apply the queryIngestersWithin time range manipulation.
	// Otherwise we'll end up returning no series at all for
	// older time ranges (while in Cortex we do ignore the start/end and always return
	// series in ingesters).
	shouldNotQueryStoreForMetadata := (sp != nil && sp.Func == "series" && !q.queryStoreForLabels)

	// If queryIngestersWithin is enabled, we do manipulate the query mint to query samples up until
	// now - queryIngestersWithin, because older time ranges are covered by the storage. This
	// optimization is particularly important for the blocks storage where the blocks retention in the
	// ingesters could be way higher than queryIngestersWithin.
	if q.queryIngestersWithin > 0 && !shouldNotQueryStoreForMetadata {
		now := time.Now()
		origMinT := minT
		minT = math.Max64(minT, util.TimeToMillis(now.Add(-q.queryIngestersWithin)))

		if origMinT != minT {
			level.Debug(log).Log("msg", "the min time of the query to ingesters has been manipulated", "original", origMinT, "updated", minT)
		}

		if minT > maxT {
			level.Debug(log).Log("msg", "empty query time range after min time manipulation")
			return storage.EmptySeriesSet()
		}
	}

	// In the recent versions of Prometheus, we pass in the hint but with Func set to "series".
	// See: https://github.com/prometheus/prometheus/pull/8050
	if sp != nil && sp.Func == "series" {
		var (
			ms  []metric.Metric
			err error
		)

		if q.streamingMetadata {
			ms, err = q.distributor.MetricsForLabelMatchersStream(ctx, model.Time(minT), model.Time(maxT), matchers...)
		} else {
			ms, err = q.distributor.MetricsForLabelMatchers(ctx, model.Time(minT), model.Time(maxT), matchers...)
		}

		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return series.MetricsToSeriesSet(sortSeries, ms)
	}

	if q.streaming {
		return q.streamingSelect(ctx, sortSeries, minT, maxT, matchers)
	}

	matrix, err := q.distributor.Query(ctx, model.Time(minT), model.Time(maxT), matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Using MatrixToSeriesSet (and in turn NewConcreteSeriesSet), sorts the series.
	return series.MatrixToSeriesSet(sortSeries, matrix)
}

func (q *distributorQuerier) streamingSelect(ctx context.Context, sortSeries bool, minT, maxT int64, matchers []*labels.Matcher) storage.SeriesSet {
	results, err := q.distributor.QueryStream(ctx, model.Time(minT), model.Time(maxT), matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// we should sort the series if we need to merge them even if sortSeries is not required by the querier
	sortSeries = sortSeries || (len(results.Timeseries) > 0 && len(results.Chunkseries) > 0)
	sets := []storage.SeriesSet(nil)
	if len(results.Timeseries) > 0 {
		sets = append(sets, newTimeSeriesSeriesSet(sortSeries, results.Timeseries))
	}

	serieses := make([]storage.Series, 0, len(results.Chunkseries))
	for _, result := range results.Chunkseries {
		// Sometimes the ingester can send series that have no data.
		if len(result.Chunks) == 0 {
			continue
		}

		ls := cortexpb.FromLabelAdaptersToLabels(result.Labels)

		chunks, err := chunkcompat.FromChunks(ls, result.Chunks)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		serieses = append(serieses, &chunkSeries{
			labels:            ls,
			chunks:            chunks,
			chunkIteratorFunc: q.chunkIterFn,
			mint:              minT,
			maxt:              maxT,
		})
	}

	if len(serieses) > 0 {
		sets = append(sets, series.NewConcreteSeriesSet(sortSeries || len(sets) > 0, serieses))
	}

	if len(sets) == 0 {
		return storage.EmptySeriesSet()
	}
	if len(sets) == 1 {
		return sets[0]
	}
	// Sets need to be sorted. Both series.NewConcreteSeriesSet and newTimeSeriesSeriesSet take care of that.
	return storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
}

func (q *distributorQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	var (
		lvs []string
		err error
	)

	if q.streamingMetadata {
		lvs, err = q.distributor.LabelValuesForLabelNameStream(q.ctx, model.Time(q.mint), model.Time(q.maxt), model.LabelName(name), matchers...)
	} else {
		lvs, err = q.distributor.LabelValuesForLabelName(q.ctx, model.Time(q.mint), model.Time(q.maxt), model.LabelName(name), matchers...)
	}

	return lvs, nil, err
}

func (q *distributorQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	if len(matchers) > 0 {
		return q.labelNamesWithMatchers(matchers...)
	}

	log, ctx := spanlogger.New(q.ctx, "distributorQuerier.LabelNames")
	defer log.Span.Finish()

	var (
		ln  []string
		err error
	)

	if q.streamingMetadata {
		ln, err = q.distributor.LabelNamesStream(ctx, model.Time(q.mint), model.Time(q.maxt))
	} else {
		ln, err = q.distributor.LabelNames(ctx, model.Time(q.mint), model.Time(q.maxt))
	}

	return ln, nil, err
}

// labelNamesWithMatchers performs the LabelNames call by calling ingester's MetricsForLabelMatchers method
func (q *distributorQuerier) labelNamesWithMatchers(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.New(q.ctx, "distributorQuerier.labelNamesWithMatchers")
	defer log.Span.Finish()

	var (
		ms  []metric.Metric
		err error
	)

	if q.streamingMetadata {
		ms, err = q.distributor.MetricsForLabelMatchersStream(ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	} else {
		ms, err = q.distributor.MetricsForLabelMatchers(ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	}

	if err != nil {
		return nil, nil, err
	}
	namesMap := make(map[string]struct{})

	for _, m := range ms {
		for name := range m.Metric {
			namesMap[string(name)] = struct{}{}
		}
	}

	names := make([]string, 0, len(namesMap))
	for name := range namesMap {
		names = append(names, name)
	}
	sort.Strings(names)

	return names, nil, nil
}

func (q *distributorQuerier) Close() error {
	return nil
}

type distributorExemplarQueryable struct {
	distributor   Distributor
	exemplarStore exemplars.ExemplarStore
}

func newDistributorExemplarQueryable(d Distributor, e exemplars.ExemplarStore) storage.ExemplarQueryable {
	return &distributorExemplarQueryable{
		distributor:   d,
		exemplarStore: e,
	}
}

func (d distributorExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &distributorExemplarQuerier{
		distributor:   d.distributor,
		exemplarStore: d.exemplarStore,
		ctx:           ctx,
	}, nil
}

type distributorExemplarQuerier struct {
	distributor   Distributor
	exemplarStore exemplars.ExemplarStore
	ctx           context.Context
}

// Select querys for exemplars, prometheus' storage.ExemplarQuerier's Select function takes the time range as two int64 values.
func (q *distributorExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	allResults, err := q.distributor.QueryExemplars(q.ctx, model.Time(start), model.Time(end), matchers...)
	if err != nil {
		return nil, err
	}

	ret := make([]exemplar.QueryResult, 0, len(allResults.Timeseries))
	var e exemplar.QueryResult
	for _, ts := range allResults.Timeseries {
		e.SeriesLabels = cortexpb.FromLabelAdaptersToLabels(ts.Labels)
		e.Exemplars = cortexpb.FromExemplarProtosToExemplars(ts.Exemplars)
		ret = append(ret, e)
	}
	if q.exemplarStore == nil {
		return ret, nil
	}

	userID, err := tenant.TenantID(q.ctx)
	if err != nil {
		return nil, err
	}
	// Remote exemplar storage.
	res, err := q.exemplarStore.Select(q.ctx, userID, start, end, matchers...)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return ret, nil
	}

	keys := make([]string, 0, len(allResults.Timeseries))
	buf := make([]byte, 0, 1024)
	dedupMap := make(map[string]exemplar.QueryResult, len(allResults.Timeseries))
	for _, ts := range allResults.Timeseries {
		key := string(cortexpb.FromLabelAdaptersToLabels(ts.Labels).Bytes(buf))
		if _, ok := dedupMap[key]; !ok {
			dedupMap[key] = exemplar.QueryResult{
				SeriesLabels: cortexpb.FromLabelAdaptersToLabels(ts.Labels),
				Exemplars:    cortexpb.FromExemplarProtosToExemplars(ts.Exemplars),
			}
			keys = append(keys, key)
		}
	}
	for _, ts := range res {
		key := string(ts.SeriesLabels.Bytes(buf))
		if qr, ok := dedupMap[key]; !ok {
			dedupMap[key] = exemplar.QueryResult{
				SeriesLabels: ts.SeriesLabels,
				Exemplars:    ts.Exemplars,
			}
			keys = append(keys, key)
		} else {
			qr.Exemplars = mergeExemplars(dedupMap[key].Exemplars, ts.Exemplars)
		}
	}
	sort.Strings(keys)

	ret = make([]exemplar.QueryResult, 0, len(dedupMap))
	for i, k := range keys {
		ret[i] = dedupMap[k]
	}
	return ret, nil
}

func mergeExemplars(exemplarsA, exemplarsB []exemplar.Exemplar) []exemplar.Exemplar {
	output := make([]exemplar.Exemplar, 0, len(exemplarsA)+len(exemplarsB))
	sort.Slice(exemplarsA, func(i, j int) bool {
		return exemplarsA[i].Ts < exemplarsA[j].Ts
	})
	sort.Slice(exemplarsB, func(i, j int) bool {
		return exemplarsB[i].Ts < exemplarsB[j].Ts
	})
	i := 0
	j := 0
	for i < len(exemplarsA) && j < len(exemplarsB) {
		t1 := exemplarsA[i].Ts
		t2 := exemplarsB[j].Ts
		if t1 == t2 {
			if exemplarsA[i].Equals(exemplarsB[j]) {
				// pick t1.
				output = append(output, exemplarsA[i])
			} else {
				// pick both
				output = append(output, exemplarsA[i])
				output = append(output, exemplarsB[j])
			}
			i++
			j++
		} else if t1 < t2 {
			output = append(output, exemplarsA[i])
			i++
		} else {
			output = append(output, exemplarsB[j])
			j++
		}
	}
	return output
}
