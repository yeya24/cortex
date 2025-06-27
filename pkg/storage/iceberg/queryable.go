package iceberg

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type IcebergQueryable struct {
	ice *IcebergStore
}

func NewIcebergQueryable(ice *IcebergStore) *IcebergQueryable {
	return &IcebergQueryable{
		ice: ice,
	}
}

func (iq *IcebergQueryable) Querier(mint, maxt int64) (prom_storage.Querier, error) {
	return &IcebergQuerier{
		mint: mint,
		maxt: maxt,
		ice:  iq.ice,
	}, nil
}

type IcebergQuerier struct {
	mint, maxt int64
	ice        *IcebergStore
}

func (iq *IcebergQuerier) LabelValues(ctx context.Context, name string, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	shards, err := iq.queryableShards(ctx, iq.mint, iq.maxt, matchers)
	if err != nil {
		return nil, nil, err
	}

	limit := int64(0)
	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameValues := make([][]string, len(shards))
	errGroup, ctx := errgroup.WithContext(ctx)

	for i, s := range shards {
		i, s := i, s // Capture variables for closure
		errGroup.Go(func() error {
			r, err := s.LabelValues(ctx, name, limit, matchers)
			resNameValues[i] = r
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, nil, err
	}

	return util.MergeUnsortedSlices(int(limit), resNameValues...), nil, nil
}

func (iq *IcebergQuerier) LabelNames(ctx context.Context, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	shards, err := iq.queryableShards(ctx, iq.mint, iq.maxt, matchers)
	if err != nil {
		return nil, nil, err
	}

	limit := int64(0)
	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameSets := make([][]string, len(shards))
	errGroup, ctx := errgroup.WithContext(ctx)

	for i, s := range shards {
		i, s := i, s // Capture variables for closure
		errGroup.Go(func() error {
			r, err := s.LabelNames(ctx, limit, matchers)
			resNameSets[i] = r
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, nil, err
	}

	return util.MergeUnsortedSlices(int(limit), resNameSets...), nil, nil
}

func (iq *IcebergQuerier) Close() error {
	return nil
}

func (iq *IcebergQuerier) Select(ctx context.Context, sorted bool, sp *prom_storage.SelectHints, matchers ...*labels.Matcher) prom_storage.SeriesSet {
	shards, err := iq.queryableShards(ctx, iq.mint, iq.maxt, matchers)
	if err != nil {
		return prom_storage.ErrSeriesSet(err)
	}
	if len(shards) == 0 {
		return prom_storage.EmptySeriesSet()
	}

	seriesSet := make([]prom_storage.SeriesSet, len(shards))

	minT, maxT := iq.mint, iq.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}
	skipChunks := sp != nil && sp.Func == "series"

	errGroup, ctx := errgroup.WithContext(ctx)

	if len(shards) > 1 {
		sorted = true
	}
	for i, shard := range shards {
		i, shard := i, shard // Capture variables for closure
		errGroup.Go(func() error {
			ss, err := shard.Query(ctx, sorted, minT, maxT, skipChunks, matchers)
			seriesSet[i] = ss
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return prom_storage.ErrSeriesSet(err)
	}

	return prom_storage.NewMergeSeriesSet(seriesSet, 0, prom_storage.ChainedSeriesMerge)
}

func extractMetricNameFromMatchers(matchers []*labels.Matcher) string {
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual {
			return matcher.Value
		}
	}
	return ""
}

func (iq *IcebergQuerier) queryableShards(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]*queryableShard, error) {
	// Extract userID from context
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Only scan the table that matches the userID (table name equals userID)
	shards, err := iq.ice.scan(ctx, userID, mint, maxt, extractMetricNameFromMatchers(matchers))
	if err != nil {
		return nil, fmt.Errorf("failed to scan table for user %s: %w", userID, err)
	}

	var allShards []*queryableShard
	for _, shard := range shards {
		qb, err := newQueryableShard(shard)
		if err != nil {
			// Log error but continue with other shards
			continue
		}
		allShards = append(allShards, qb)
	}

	return allShards, nil
}

type queryableShard struct {
	shard *ParquetShard
	m     *Materializer
}

func newQueryableShard(shard *ParquetShard) (*queryableShard, error) {
	// Create materializer with column indices
	m := &Materializer{
		shard: shard,
	}

	// Initialize column indices using lookup
	if err := m.initializeColumnIndices(); err != nil {
		return nil, fmt.Errorf("failed to initialize column indices: %w", err)
	}

	return &queryableShard{
		shard: shard,
		m:     m,
	}, nil
}

func equalNameMatcher(matchers ...*labels.Matcher) (*labels.Matcher, []*labels.Matcher) {
	others := make([]*labels.Matcher, 0, len(matchers))
	var metricNameMatcher *labels.Matcher
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			metricNameMatcher = m
		} else {
			others = append(others, m)
		}
	}
	return metricNameMatcher, others
}

func (b *queryableShard) Query(ctx context.Context, sorted bool, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.SeriesSet, error) {
	// For now, return empty result
	// This should be implemented to actually query the parquet file
	// The full implementation would require proper integration with the parquet-common search package

	metricNameMatcher, otherMatchers := equalNameMatcher(matchers...)

	errGroup, ctx := errgroup.WithContext(ctx)
	results := make([]prom_storage.Series, 0, 1024)
	rMtx := sync.Mutex{}
	for i, rg := range b.shard.f.RowGroups() {
		errGroup.Go(func() error {
			rr := []RowRange{{from: int64(0), count: rg.NumRows()}}
			rr, err := b.SearchMetricName(ctx, rg, metricNameMatcher.Value, rr)
			if err != nil {
				return err
			}
			if len(rr) == 0 {
				return nil
			}
			rr, err = b.SearchTimestamp(ctx, rg, mint, maxt, rr)
			if err != nil {
				return err
			}
			if len(rr) == 0 {
				return nil
			}
			rr, err = b.SearchLabels(ctx, rg, otherMatchers, rr)
			if err != nil {
				return err
			}
			if len(rr) == 0 {
				return nil
			}

			series, err := b.m.Materialize(ctx, i, skipChunks, rr)
			if err != nil {
				return err
			}
			rMtx.Lock()
			results = append(results, series...)
			rMtx.Unlock()
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	if sorted {
		sort.Sort(byLabels(results))
	}
	return NewSeriesSet(results), nil
}

func (b *queryableShard) LabelNames(ctx context.Context, limit int64, matchers []*labels.Matcher) ([]string, error) {
	// For now, return empty result
	// This should be implemented to actually query label names from the parquet file
	return []string{}, nil
}

func (b *queryableShard) LabelValues(ctx context.Context, name string, limit int64, matchers []*labels.Matcher) ([]string, error) {
	// For now, return empty result
	// This should be implemented to actually query label values from the parquet file
	return []string{}, nil
}

func (b *queryableShard) allLabelValues(ctx context.Context, name string, limit int64) ([]string, error) {
	// For now, return empty result
	// This should be implemented to actually query all label values from the parquet file
	return []string{}, nil
}

type byLabels []prom_storage.Series

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }

// Materializer is a placeholder for the actual materialization logic
type Materializer struct {
	shard *ParquetShard
	// Column indices for each column
	columnIndices map[string]int
}

// initializeColumnIndices initializes the column indices using the parquet schema lookup
func (m *Materializer) initializeColumnIndices() error {
	if len(m.shard.f.RowGroups()) == 0 {
		return fmt.Errorf("no row groups found in parquet file")
	}

	rg := m.shard.f.RowGroups()[0] // Use first row group to get schema
	schema := rg.Schema()

	m.columnIndices = make(map[string]int)

	// Look up each column and store its index
	columns := []string{"metric_name", "timestamp", "value", "labels"}
	for _, colName := range columns {
		col, ok := schema.Lookup(colName)
		if !ok {
			return fmt.Errorf("column %s not found in parquet schema", colName)
		}
		m.columnIndices[colName] = col.ColumnIndex
	}

	return nil
}

// Materialize materializes series from the parquet file
func (m *Materializer) Materialize(ctx context.Context, rgi int, skipChunks bool, rr []RowRange) ([]prom_storage.Series, error) {
	rg := m.shard.f.RowGroups()[rgi]
	columnChunks := rg.ColumnChunks()

	// Get column chunks using stored indices
	metricNameCol := columnChunks[m.columnIndices["metric_name"]]
	timestampCol := columnChunks[m.columnIndices["timestamp"]]
	valueCol := columnChunks[m.columnIndices["value"]]
	labelsCol := columnChunks[m.columnIndices["labels"]]

	// Read pages for each column
	metricNamePages, err := m.shard.f.GetPages(ctx, metricNameCol)
	if err != nil {
		return nil, fmt.Errorf("failed to get metric name pages: %w", err)
	}
	defer metricNamePages.Close()

	timestampPages, err := m.shard.f.GetPages(ctx, timestampCol)
	if err != nil {
		return nil, fmt.Errorf("failed to get timestamp pages: %w", err)
	}
	defer timestampPages.Close()

	labelsPages, err := m.shard.f.GetPages(ctx, labelsCol)
	if err != nil {
		return nil, fmt.Errorf("failed to get labels pages: %w", err)
	}
	defer labelsPages.Close()

	// Only read value pages if not skipping chunks
	var valuePages parquet.Pages
	if !skipChunks {
		valuePages, err = m.shard.f.GetPages(ctx, valueCol)
		if err != nil {
			return nil, fmt.Errorf("failed to get value pages: %w", err)
		}
		defer valuePages.Close()
	}

	// Group rows by metric name and labels to create series
	seriesMap := make(map[string]*parquetSeries)

	// Read only the rows in the filtered row ranges (no additional filtering needed)
	for _, rowRange := range rr {
		for row := rowRange.from; row < rowRange.from+rowRange.count; row++ {
			// Read metric name
			if err := metricNamePages.SeekToRow(row); err != nil {
				continue // Skip this row if we can't read
			}
			metricNamePage, err := metricNamePages.ReadPage()
			if err != nil {
				continue
			}
			rowMetricName := m.readStringValue(metricNamePage, 0)

			// Read timestamp
			if err := timestampPages.SeekToRow(row); err != nil {
				continue
			}
			timestampPage, err := timestampPages.ReadPage()
			if err != nil {
				continue
			}
			timestamp := m.readInt64Value(timestampPage, 0)

			// Read labels
			if err := labelsPages.SeekToRow(row); err != nil {
				continue
			}
			labelsPage, err := labelsPages.ReadPage()
			if err != nil {
				continue
			}
			labelsJSON := m.readBytesValue(labelsPage, 0)

			// Parse labels
			var lbls labels.Labels
			if len(labelsJSON) > 0 {
				if err := lbls.UnmarshalJSON(labelsJSON); err != nil {
					continue // Skip this row if we can't parse labels
				}
			}

			// Create series key (metric name + labels hash)
			seriesKey := fmt.Sprintf("%s:%d", rowMetricName, lbls.Hash())

			// Get or create series
			series, exists := seriesMap[seriesKey]
			if !exists {
				// Create new series with metric name and labels
				seriesLabels := labels.NewBuilder(lbls)
				seriesLabels.Set(labels.MetricName, rowMetricName)

				series = &parquetSeries{
					labels:  seriesLabels.Labels(),
					samples: make([]cortexpb.Sample, 0),
				}
				seriesMap[seriesKey] = series
			}

			// Read value only if not skipping chunks
			if !skipChunks {
				if err := valuePages.SeekToRow(row); err != nil {
					continue
				}
				valuePage, err := valuePages.ReadPage()
				if err != nil {
					continue
				}
				value := m.readFloat64Value(valuePage, 0)

				// Add sample to series
				series.samples = append(series.samples, cortexpb.Sample{
					TimestampMs: timestamp,
					Value:       value,
				})
			} else {
				// When skipping chunks, just add a placeholder sample with timestamp only
				series.samples = append(series.samples, cortexpb.Sample{
					TimestampMs: timestamp,
					Value:       0, // Placeholder value when skipping chunks
				})
			}
		}
	}

	// Convert map to slice of series
	var result []prom_storage.Series
	for _, series := range seriesMap {
		// Sort samples by timestamp
		sort.Slice(series.samples, func(i, j int) bool {
			return series.samples[i].TimestampMs < series.samples[j].TimestampMs
		})
		result = append(result, series)
	}

	return result, nil
}

// parquetSeries implements prom_storage.Series
type parquetSeries struct {
	labels  labels.Labels
	samples []cortexpb.Sample
}

func (p *parquetSeries) Labels() labels.Labels {
	return p.labels
}

func (p *parquetSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if csi, ok := it.(*parquetSeriesIterator); ok {
		csi.reset(p)
		return csi
	}
	return newParquetSeriesIterator(p)
}

// parquetSeriesIterator implements chunkenc.Iterator
type parquetSeriesIterator struct {
	series *parquetSeries
	cur    int
}

func newParquetSeriesIterator(series *parquetSeries) chunkenc.Iterator {
	return &parquetSeriesIterator{
		series: series,
		cur:    -1,
	}
}

func (p *parquetSeriesIterator) reset(series *parquetSeries) {
	p.series = series
	p.cur = -1
}

func (p *parquetSeriesIterator) Seek(t int64) chunkenc.ValueType {
	// Binary search for the first sample >= t
	p.cur = sort.Search(len(p.series.samples), func(i int) bool {
		return p.series.samples[i].TimestampMs >= t
	})

	if p.cur >= len(p.series.samples) {
		return chunkenc.ValNone
	}
	return chunkenc.ValFloat
}

func (p *parquetSeriesIterator) Next() chunkenc.ValueType {
	p.cur++
	if p.cur >= len(p.series.samples) {
		return chunkenc.ValNone
	}
	return chunkenc.ValFloat
}

func (p *parquetSeriesIterator) At() (t int64, v float64) {
	if p.cur < 0 || p.cur >= len(p.series.samples) {
		return 0, 0
	}
	sample := p.series.samples[p.cur]
	return sample.TimestampMs, sample.Value
}

func (p *parquetSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("histograms not supported")
}

func (p *parquetSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("histograms not supported")
}

func (p *parquetSeriesIterator) AtT() int64 {
	if p.cur < 0 || p.cur >= len(p.series.samples) {
		return 0
	}
	return p.series.samples[p.cur].TimestampMs
}

func (p *parquetSeriesIterator) Err() error {
	return nil
}

// Helper methods to read values from parquet pages
func (m *Materializer) readStringValue(pg parquet.Page, index int) string {
	vi := new(valuesIterator)
	vi.Reset(pg)
	for i := 0; i <= index; i++ {
		if !vi.Next() {
			return ""
		}
	}
	return vi.At().String()
}

func (m *Materializer) readInt64Value(pg parquet.Page, index int) int64 {
	vi := new(valuesIterator)
	vi.Reset(pg)
	for i := 0; i <= index; i++ {
		if !vi.Next() {
			return 0
		}
	}
	return vi.At().Int64()
}

func (m *Materializer) readFloat64Value(pg parquet.Page, index int) float64 {
	vi := new(valuesIterator)
	vi.Reset(pg)
	for i := 0; i <= index; i++ {
		if !vi.Next() {
			return 0
		}
	}
	return vi.At().Double()
}

func (m *Materializer) readBytesValue(pg parquet.Page, index int) []byte {
	vi := new(valuesIterator)
	vi.Reset(pg)
	for i := 0; i <= index; i++ {
		if !vi.Next() {
			return nil
		}
	}
	return vi.At().Bytes()
}

func labelsMatches(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, matcher := range matchers {
		if val := lbls.Get(matcher.Name); val != "" {
			if !matcher.Matches(val) {
				return false
			}
		}
	}
	return true
}

// MaterializeAllLabelNames materializes all label names from the parquet file
func (m *Materializer) MaterializeAllLabelNames() []string {
	// For now, return empty result
	// This should be implemented to actually materialize label names from the parquet file
	return []string{}
}

// MaterializeLabelNames materializes label names from the parquet file based on row ranges
func (m *Materializer) MaterializeLabelNames(ctx context.Context, rowGroupIndex int, rr []int) ([]string, error) {
	// For now, return empty result
	// This should be implemented to actually materialize label names from the parquet file
	return []string{}, nil
}

// MaterializeLabelValues materializes label values from the parquet file based on row ranges
func (m *Materializer) MaterializeLabelValues(ctx context.Context, name string, rowGroupIndex int, rr []int) ([]string, error) {
	// For now, return empty result
	// This should be implemented to actually materialize label values from the parquet file
	return []string{}, nil
}

// MaterializeAllLabelValues materializes all label values for a given name from the parquet file
func (m *Materializer) MaterializeAllLabelValues(ctx context.Context, name string, rowGroupIndex int) ([]string, error) {
	// For now, return empty result
	// This should be implemented to actually materialize all label values from the parquet file
	return []string{}, nil
}

type symbolTable struct {
	dict parquet.Dictionary
	syms []int32
}

func (s *symbolTable) Get(i int) parquet.Value {
	switch s.syms[i] {
	case -1:
		return parquet.NullValue()
	default:
		return s.dict.Index(s.syms[i])
	}
}

func (s *symbolTable) GetIndex(i int) int32 {
	return s.syms[i]
}

func (s *symbolTable) Reset(pg parquet.Page) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	defs := pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(defs))
	} else {
		s.syms = slices.Grow(s.syms, len(defs))[:len(defs)]
	}

	sidx := 0
	for i := range defs {
		if defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		} else {
			s.syms[i] = -1
		}
	}
	s.dict = dict
}

type valuesIterator struct {
	p parquet.Page

	// TODO: consider using unique.Handle
	cachedSymbols map[int32]parquet.Value
	st            symbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (vi *valuesIterator) Reset(p parquet.Page) {
	vi.p = p
	vi.vr = nil
	if p.Dictionary() != nil {
		vi.st.Reset(p)
		vi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		vi.vr = p.Values()
		vi.buffer = make([]parquet.Value, 0, 128)
		vi.currentBufferIndex = -1
	}
	vi.current = -1
}

func (vi *valuesIterator) Next() bool {
	if vi.err != nil {
		return false
	}

	vi.current++
	if vi.current >= int(vi.p.NumRows()) {
		return false
	}

	vi.currentBufferIndex++

	if vi.currentBufferIndex == len(vi.buffer) {
		n, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
		if err != nil && err != io.EOF {
			vi.err = err
		}
		vi.buffer = vi.buffer[:n]
		vi.currentBufferIndex = 0
	}

	return true
}

func (vi *valuesIterator) Error() error {
	return vi.err
}

func (vi *valuesIterator) At() parquet.Value {
	if vi.vr == nil {
		dicIndex := vi.st.GetIndex(vi.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := vi.cachedSymbols[dicIndex]; !ok {
			vi.cachedSymbols[dicIndex] = vi.st.Get(vi.current).Clone()
		}
		return vi.cachedSymbols[dicIndex]
	}

	return vi.buffer[vi.currentBufferIndex].Clone()
}

func (m *queryableShard) SearchTimestamp(ctx context.Context, rg parquet.RowGroup, mint, maxt int64, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	// Use stored column index for timestamp
	colIndex, ok := m.m.columnIndices["timestamp"]
	if !ok {
		return []RowRange{}, nil
	}

	cc := rg.ColumnChunks()[colIndex]

	// Look up the column to get the type for comparison
	col, ok := rg.Schema().Lookup("timestamp")
	if !ok {
		return []RowRange{}, nil
	}
	comp := col.Node.Type().Compare

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get column index")
	}

	minT := parquet.ValueOf(mint)
	maxT := parquet.ValueOf(maxt)

	var (
		res = make([]RowRange, 0)
	)

	pgs, err := m.shard.f.GetPages(ctx, cc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}
	defer func() { _ = pgs.Close() }()

	for i := 0; i < cidx.NumPages(); i++ {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < cidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			continue
		}
		// If we are not matching the empty string ( which would be satisfied by Null too ), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !maxv.IsNull() && comp(minT, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !minv.IsNull() && comp(maxT, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))

		vi := new(valuesIterator)
		vi.Reset(pg)
		off, count := bl, 0
		for j := int(pfrom); j < bl; j++ {
			if !vi.Next() {
				if vi.Error() != nil {
					return nil, vi.Error()
				}
				break
			}
		}
		for j := bl; j < br; j++ {
			if !vi.Next() {
				if vi.Error() != nil {
					return nil, vi.Error()
				}
				break
			}
			val := vi.At().Int64()
			if val < mint || val > maxt {
				if count != 0 {
					res = append(res, RowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, RowRange{pfrom + int64(off), int64(count)})
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (m *queryableShard) SearchLabels(ctx context.Context, rg parquet.RowGroup, matchers []*labels.Matcher, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	// Use stored column index for labels
	colIndex, ok := m.m.columnIndices["labels"]
	if !ok {
		return []RowRange{}, nil
	}

	cc := rg.ColumnChunks()[colIndex]
	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get column index")
	}

	var (
		res = make([]RowRange, 0)
	)

	pgs, err := m.shard.f.GetPages(ctx, cc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}
	defer func() { _ = pgs.Close() }()

	for i := 0; i < cidx.NumPages(); i++ {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < cidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		vi := new(valuesIterator)
		vi.Reset(pg)
		off, count := bl, 0
		for j := int(pfrom); j < bl; j++ {
			if !vi.Next() {
				if vi.Error() != nil {
					return nil, vi.Error()
				}
				break
			}
		}
		for j := bl; j < br; j++ {
			if !vi.Next() {
				if vi.Error() != nil {
					return nil, vi.Error()
				}
				break
			}
			val := vi.At().Bytes()
			var lbls labels.Labels
			if err := lbls.UnmarshalJSON(val); err != nil {
				return nil, fmt.Errorf("unable to unmarshal labels: %w", err)
			}
			if !labelsMatches(lbls, matchers) {
				if count != 0 {
					res = append(res, RowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, RowRange{pfrom + int64(off), int64(count)})
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (m *queryableShard) SearchMetricName(ctx context.Context, rg parquet.RowGroup, metricName string, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	// Use stored column index for metric_name
	colIndex, ok := m.m.columnIndices["metric_name"]
	if !ok {
		return []RowRange{}, nil
	}

	cc := rg.ColumnChunks()[colIndex]

	// Look up the column to get the type for comparison
	col, ok := rg.Schema().Lookup("metric_name")
	if !ok {
		return []RowRange{}, nil
	}
	comp := col.Node.Type().Compare

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get column index")
	}

	pv := parquet.ValueOf(metricName)

	var (
		res = make([]RowRange, 0)
	)

	pgs, err := m.shard.f.GetPages(ctx, cc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}
	defer func() { _ = pgs.Close() }()

	for i := 0; i < cidx.NumPages(); i++ {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < cidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			continue
		}
		// If we are not matching the empty string ( which would be satisfied by Null too ), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !maxv.IsNull() && comp(pv, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !minv.IsNull() && comp(pv, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		vi := new(valuesIterator)
		vi.Reset(pg)
		off, count := bl, 0
		for j := int(pfrom); j < bl; j++ {
			if !vi.Next() {
				if vi.Error() != nil {
					return nil, vi.Error()
				}
				break
			}
		}
		for j := bl; j < br; j++ {
			if !vi.Next() {
				if vi.Error() != nil {
					return nil, vi.Error()
				}
				break
			}
			val := vi.At().String()
			if metricName != val {
				if count != 0 {
					res = append(res, RowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, RowRange{pfrom + int64(off), int64(count)})
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}
