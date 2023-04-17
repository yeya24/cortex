package querier

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-community/promql-engine/api"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	grpc_metadata "google.golang.org/grpc/metadata"
)

type TenantStoreGatewayEngines struct {
	finder               BlocksFinder
	stores               BlocksStoreSet
	clientsPool          *client.Pool
	queryIngestersWithin time.Duration
	queryStoreAfter      time.Duration
}

func NewTenantStoreGatewayEngines(finder BlocksFinder, stores BlocksStoreSet, clientsPool *client.Pool, queryIngesterWithin, queryStoreAfter time.Duration) *TenantStoreGatewayEngines {
	return &TenantStoreGatewayEngines{
		finder:               finder,
		stores:               stores,
		clientsPool:          clientsPool,
		queryIngestersWithin: queryIngesterWithin,
		queryStoreAfter:      queryStoreAfter,
	}
}

func findMinMaxTime(s *parser.EvalStmt) (int64, int64) {
	var minTimestamp, maxTimestamp int64 = math.MaxInt64, math.MinInt64
	// Whenever a MatrixSelector is evaluated, evalRange is set to the corresponding range.
	// The evaluation of the VectorSelector inside then evaluates the given range and unsets
	// the variable.
	var evalRange time.Duration
	parser.Inspect(s.Expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			start, end := getTimeRangesForSelector(s, n, path, evalRange)
			if start < minTimestamp {
				minTimestamp = start
			}
			if end > maxTimestamp {
				maxTimestamp = end
			}
			evalRange = 0

		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})

	if maxTimestamp == math.MinInt64 {
		// This happens when there was no selector. Hence no time range to select.
		minTimestamp = 0
		maxTimestamp = 0
	}

	return minTimestamp, maxTimestamp
}

func getTimeRangesForSelector(s *parser.EvalStmt, n *parser.VectorSelector, path []parser.Node, evalRange time.Duration) (int64, int64) {
	start, end := timestamp.FromTime(s.Start), timestamp.FromTime(s.End)
	subqOffset, subqRange, subqTs := subqueryTimes(path)

	if subqTs != nil {
		// The timestamp on the subquery overrides the eval statement time ranges.
		start = *subqTs
		end = *subqTs
	}

	if n.Timestamp != nil {
		// The timestamp on the selector overrides everything.
		start = *n.Timestamp
		end = *n.Timestamp
	} else {
		offsetMilliseconds := durationMilliseconds(subqOffset)
		start = start - offsetMilliseconds - durationMilliseconds(subqRange)
		end = end - offsetMilliseconds
	}

	if evalRange == 0 {
		start = start - durationMilliseconds(s.LookbackDelta)
	} else {
		// For all matrix queries we want to ensure that we have (end-start) + range selected
		// this way we have `range` data before the start time
		start = start - durationMilliseconds(evalRange)
	}

	offsetMilliseconds := durationMilliseconds(n.OriginalOffset)
	start = start - offsetMilliseconds
	end = end - offsetMilliseconds

	return start, end
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func subqueryTimes(path []parser.Node) (time.Duration, time.Duration, *int64) {
	var (
		subqOffset, subqRange time.Duration
		ts                    int64 = math.MaxInt64
	)
	for _, node := range path {
		switch n := node.(type) {
		case *parser.SubqueryExpr:
			subqOffset += n.OriginalOffset
			subqRange += n.Range
			if n.Timestamp != nil {
				// The @ modifier on subquery invalidates all the offset and
				// range till now. Hence resetting it here.
				subqOffset = n.OriginalOffset
				subqRange = n.Range
				ts = *n.Timestamp
			}
		}
	}
	var tsp *int64
	if ts != math.MaxInt64 {
		tsp = &ts
	}
	return subqOffset, subqRange, tsp
}

func (m *TenantStoreGatewayEngines) Engines(userID string, query string, start, end time.Time, lookbackDelta time.Duration) []api.RemoteEngine {
	now := time.Now()
	e, err := parser.ParseExpr(query)
	if err != nil {
		return nil
	}
	mint, maxt := findMinMaxTime(&parser.EvalStmt{
		Expr:          e,
		Start:         start,
		End:           end,
		LookbackDelta: lookbackDelta,
	})

	// Need to query ingesters, skipping pushing down.
	if m.queryIngestersWithin == 0 || maxt >= util.TimeToMillis(now.Add(-m.queryIngestersWithin)) {
		return nil
	}

	// Include this store only if mint is within QueryStoreAfter w.r.t current time.
	if m.queryStoreAfter != 0 && mint > util.TimeToMillis(now.Add(-m.queryStoreAfter)) {
		return nil
	}

	knownBlocks, _, err := m.finder.GetBlocks(context.Background(), userID, mint, maxt)
	if err != nil {

	}
	requiredLen := len(knownBlocks.GetULIDs())
	res := make([]api.RemoteEngine, 0)
	blockLocations, err := m.stores.GetBlocksLocations(userID, knownBlocks.GetULIDs())
	for addr, blocks := range blockLocations {
		if len(blocks) == requiredLen {
			c, err := m.clientsPool.GetClientFor(addr)
			if err != nil {
				continue
			}
			res = append(res, &StoreGatewayRemoteEngine{
				Addr:     addr,
				BlockIDs: convertULIDToString(blocks),
				client:   c.(storegatewaypb.StoreGatewayClient),
				userID:   userID,
			})
		}
	}
	return res
}

func convertULIDToString(ulids []ulid.ULID) []string {
	output := make([]string, 0, len(ulids))
	for _, u := range ulids {
		output = append(output, u.String())
	}
	return output
}

type StoreGatewayRemoteEngine struct {
	Addr     string
	BlockIDs []string
	client   storegatewaypb.StoreGatewayClient
	userID   string
}

func (e *StoreGatewayRemoteEngine) MaxT() int64 { return 0 }

func (e *StoreGatewayRemoteEngine) MinT() int64 { return 0 }

func (e *StoreGatewayRemoteEngine) LabelSets() []labels.Labels { return nil }

func (e *StoreGatewayRemoteEngine) NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	return &remoteQuery{
		query:         qs,
		start:         start,
		end:           end,
		step:          step,
		blockIDs:      e.BlockIDs,
		lookBackDelta: opts.LookbackDelta,
		client:        e.client,
		userID:        e.userID,
		logger:        util_log.Logger,
	}, nil
}

type Opts struct {
	Timeout time.Duration
}

type remoteQuery struct {
	logger log.Logger
	client storegatewaypb.StoreGatewayClient
	opts   Opts

	query         string
	start         time.Time
	end           time.Time
	step          time.Duration
	lookBackDelta time.Duration

	blockIDs []string
	cancel   context.CancelFunc
	userID   string
}

func (r *remoteQuery) Exec(ctx context.Context) *promql.Result {
	start := time.Now()

	reqCtx := grpc_metadata.AppendToOutgoingContext(ctx, cortex_tsdb.TenantIDExternalLabel, r.userID)
	qctx, cancel := context.WithCancel(reqCtx)
	r.cancel = cancel
	defer cancel()

	request := &storegatewaypb.QueryRangeRequest{
		Query:                r.query,
		StartTimeSeconds:     r.start.Unix(),
		EndTimeSeconds:       r.end.Unix(),
		IntervalSeconds:      int64(r.step.Seconds()),
		TimeoutSeconds:       int64(r.opts.Timeout.Seconds()),
		LookbackDeltaSeconds: int64(r.lookBackDelta.Seconds()),
		BlockIDs:             r.blockIDs,
	}

	qry, err := r.client.QueryRange(qctx, request)
	if err != nil {
		return &promql.Result{Err: err}
	}

	result := make(promql.Matrix, 0)
	for {
		msg, err := qry.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &promql.Result{Err: err}
		}

		if warn := msg.GetWarnings(); warn != "" {
			return &promql.Result{Err: errors.New(warn)}
		}

		ts := msg.GetTimeseries()
		if ts == nil {
			continue
		}
		series := promql.Series{
			Metric: cortexpb.FromLabelAdaptersToLabels(ts.Labels),
			Points: make([]promql.Point, 0, len(ts.Samples)),
		}
		for _, s := range ts.Samples {
			series.Points = append(series.Points, promql.Point{
				T: s.TimestampMs,
				V: s.Value,
			})
		}
		result = append(result, series)
	}
	level.Debug(r.logger).Log("Executed query", "query", r.query, "time", time.Since(start))

	return &promql.Result{Value: result}
}

func (r *remoteQuery) Close() {
	r.Cancel()
}

func (r *remoteQuery) Statement() parser.Statement {
	return nil
}

func (r *remoteQuery) Stats() *stats.Statistics {
	return nil
}

func (r *remoteQuery) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *remoteQuery) String() string {
	return r.query
}
