package iceberg

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/storage/frostdb"
	util2 "github.com/cortexproject/cortex/pkg/util"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus-community/parquet-common/storage"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

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
	shards, err := iq.ice.scan(ctx, userID, mint, maxt, matchers)
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

func LabelToColumn(lbl string) string {
	return fmt.Sprintf("labels.%s", lbl)
}

func MatchersToConstraint(matchers ...*labels.Matcher) ([]search.Constraint, error) {
	r := make([]search.Constraint, 0, len(matchers))
	for _, matcher := range matchers {
		name := LabelToColumn(matcher.Name)
		switch matcher.Type {
		case labels.MatchEqual:
			r = append(r, search.Equal(name, parquet.ValueOf(matcher.Value)))
		case labels.MatchNotEqual:
			r = append(r, search.Not(search.Equal(name, parquet.ValueOf(matcher.Value))))
		case labels.MatchRegexp:
			res, err := labels.NewFastRegexMatcher(matcher.Value)
			if err != nil {
				return nil, err
			}
			r = append(r, search.Regex(name, res))
		case labels.MatchNotRegexp:
			res, err := labels.NewFastRegexMatcher(matcher.Value)
			if err != nil {
				return nil, err
			}
			r = append(r, search.Not(search.Regex(name, res)))
		default:
			return nil, fmt.Errorf("unsupported matcher type %s", matcher.Type)
		}
	}
	return r, nil
}

func (b *queryableShard) Query(ctx context.Context, sorted bool, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.SeriesSet, error) {
	// For now, return empty result
	// This should be implemented to actually query the parquet file
	// The full implementation would require proper integration with the parquet-common search package

	errGroup, ctx := errgroup.WithContext(ctx)
	results := make([]prom_storage.SeriesSet, 0)
	rMtx := sync.Mutex{}
	numRowGroups := b.shard.buf.NumRowGroups()
	for i := 0; i < numRowGroups; i++ {
		rg := b.shard.buf.DynamicRowGroup(i)
		errGroup.Go(func() error {
			cs, err := MatchersToConstraint(matchers...)
			if err != nil {
				return err
			}
			if err := search.Initialize(b.shard.f, cs...); err != nil {
				return err
			}
			rr, err := search.Filter(ctx, rg, cs...)
			if err != nil {
				return err
			}
			start := time.Now()
			series, err := b.m.Materialize(ctx, i, skipChunks, rr)
			if err != nil {
				return err
			}
			fmt.Printf("materialized %d series took %v\n", len(series), time.Since(start))
			if sorted {
				sort.Sort(byLabels(series))
			}
			rMtx.Lock()
			results = append(results, NewSeriesSet(series))
			rMtx.Unlock()
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return prom_storage.NewMergeSeriesSet(results, 0, prom_storage.ChainedSeriesMerge), nil
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
	colIdx        int
	partitioner   util.Partitioner
}

// initializeColumnIndices initializes the column indices using the parquet schema lookup
func (m *Materializer) initializeColumnIndices() error {
	if m.shard.buf.NumRowGroups() == 0 {
		return fmt.Errorf("no row groups found in parquet file")
	}

	m.columnIndices = make(map[string]int)

	// Look up each column and store its index
	for i, colNames := range m.shard.buf.ParquetFile().Schema().Columns() {
		m.columnIndices[colNames[0]] = i
	}
	colIdx, ok := m.shard.buf.ParquetFile().Schema().Lookup(frostdb.ColumnIndices)
	if !ok {
		return fmt.Errorf("no column indices found in parquet file")
	}
	m.colIdx = colIdx.ColumnIndex
	m.partitioner = util.NewGapBasedPartitioner(512 * 1024)

	return nil
}

func (m *Materializer) Materialize(ctx context.Context, rgi int, skipChunks bool, rr []search.RowRange) ([]prom_storage.Series, error) {
	newRR := make([]RowRange, 0, len(rr))
	for _, r := range rr {
		newRR = append(newRR, RowRange{
			from:  r.From(),
			count: r.Count(),
		})
	}
	errGroup, ctx := errgroup.WithContext(ctx)
	rg := m.shard.f.RowGroups()[rgi]
	var (
		timestamps []parquet.Value
		vals       []parquet.Value
		sLbls      []labels.Labels
	)
	errGroup.Go(func() error {
		lbls, err := m.materializeAllLabels(ctx, rgi, newRR)
		if err != nil {
			return errors.Wrapf(err, "error materializing labels")
		}
		sLbls = lbls
		return nil
	})
	if !skipChunks {
		errGroup.Go(func() error {
			cc := rg.ColumnChunks()[m.columnIndices[frostdb.ColumnTimestamp]]
			values, err := m.materializeColumn(ctx, m.shard.f, rg, cc, newRR)
			if err != nil {
				return errors.Wrap(err, "failed to materialize timestamp")
			}
			timestamps = values
			return nil
		})

		errGroup.Go(func() error {
			cc := rg.ColumnChunks()[m.columnIndices[frostdb.ColumnValue]]
			values, err := m.materializeColumn(ctx, m.shard.f, rg, cc, newRR)
			if err != nil {
				return errors.Wrap(err, "failed to materialize values")
			}
			vals = values
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	if !skipChunks {
		if len(timestamps) != len(vals) {
			return nil, fmt.Errorf("expected %d values, got %d", len(timestamps), len(vals))
		}
	}
	results := make([]uint64, len(sLbls))
	seriesMap := make(map[uint64]prom_storage.Series, len(sLbls))
	for i, s := range sLbls {
		sort.Sort(s)
		h := s.Hash()
		if _, ok := seriesMap[h]; !ok {
			seriesMap[h] = &parquetSeries{
				labels:  s,
				samples: make([]cortexpb.Sample, 0),
			}
		}
		results[i] = h
	}
	for i, ts := range timestamps {
		ps := seriesMap[results[i]].(*parquetSeries)
		ps.samples = append(ps.samples, cortexpb.Sample{
			TimestampMs: ts.Int64(),
			Value:       vals[i].Double(),
		})
	}

	res := make([]prom_storage.Series, 0, len(seriesMap))
	for _, s := range seriesMap {
		res = append(res, s)
	}
	return res, nil
}

type pageEntryRead struct {
	pages []int
	rows  []RowRange
}

// Merge nearby pages to enable efficient sequential reads.
// Pages that are not close to each other will be scheduled for concurrent reads.
func (m *Materializer) coalescePageRanges(pagedIdx map[int][]RowRange, offset parquet.OffsetIndex) []pageEntryRead {
	if len(pagedIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := m.partitioner.Partition(len(idxs), func(i int) (int, int) {
		return int(offset.Offset(idxs[i])), int(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageEntryRead, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageEntryRead{}
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			pagesToRead.pages = append(pagesToRead.pages, idxs[i])
			pagesToRead.rows = append(pagesToRead.rows, pagedIdx[idxs[i]]...)
		}
		pagesToRead.rows = simplify(pagesToRead.rows)
		r = append(r, pagesToRead)
	}

	return r
}

func (m *Materializer) materializeColumn(ctx context.Context, file *storage.ParquetFile, group parquet.RowGroup, cc parquet.ColumnChunk, rr []RowRange) ([]parquet.Value, error) {
	if len(rr) == 0 {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get column index")
	}

	pagesToRowsMap := make(map[int][]RowRange, len(rr))

	for i := 0; i < cidx.NumPages(); i++ {
		pageRowRange := RowRange{
			from: oidx.FirstRowIndex(i),
		}
		pageRowRange.count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.count = oidx.FirstRowIndex(i+1) - pageRowRange.from
		}

		for _, r := range rr {
			if pageRowRange.Overlaps(r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], pageRowRange.Intersection(r))
			}
		}
	}

	pageRanges := m.coalescePageRanges(pagesToRowsMap, oidx)

	r := make(map[RowRange][]parquet.Value, len(pageRanges))
	rMutex := &sync.Mutex{}
	for _, v := range pageRanges {
		for _, rs := range v.rows {
			r[rs] = make([]parquet.Value, 0, rs.count)
		}
	}

	errGroup := &errgroup.Group{}
	for _, p := range pageRanges {
		errGroup.Go(func() error {
			pgs, err := file.GetPages(ctx, cc, p.pages...)
			if err != nil {
				return errors.Wrap(err, "failed to get pages")
			}
			defer func() { _ = pgs.Close() }()
			err = pgs.SeekToRow(p.rows[0].from)
			if err != nil {
				return errors.Wrap(err, "could not seek to row")
			}

			vi := new(valuesIterator)
			remainingRr := p.rows
			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from

			remainingRr = remainingRr[1:]
			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					return errors.Wrap(err, "could not read page")
				}
				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						rMutex.Lock()
						r[currentRr] = append(r[currentRr], vi.At())
						rMutex.Unlock()
						remaining--
						if remaining > 0 {
							next = next + 1
						} else if len(remainingRr) > 0 {
							currentRr = remainingRr[0]
							next = currentRr.from
							remaining = currentRr.count
							remainingRr = remainingRr[1:]
						}
					}
					currentRow++
				}
				parquet.Release(page)

				if vi.Error() != nil {
					return vi.Error()
				}
			}
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to materialize columns")
	}

	ranges := slices.Collect(maps.Keys(r))
	slices.SortFunc(ranges, func(a, b RowRange) int {
		return int(a.from - b.from)
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range ranges {
		res = append(res, r[v]...)
	}
	return res, nil
}

func totalRows(rr []RowRange) int64 {
	res := int64(0)
	for _, r := range rr {
		res += r.count
	}
	return res
}

func (m *Materializer) materializeAllLabels(ctx context.Context, rgi int, rr []RowRange) ([]labels.Labels, error) {
	labelsRg := m.shard.f.RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumn(ctx, m.shard.f, labelsRg, cc, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	colsMap := make(map[int]*[]parquet.Value, 10)
	results := make([]labels.Labels, len(colsIdxs))

	for _, colsIdx := range colsIdxs {
		idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to decode column index")
		}
		for _, idx := range idxs {
			colsMap[idx] = &[]parquet.Value{}
		}
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	for cIdx, v := range colsMap {
		errGroup.Go(func() error {
			cc := labelsRg.ColumnChunks()[cIdx]
			values, err := m.materializeColumn(ctx, m.shard.f, labelsRg, cc, rr)
			if err != nil {
				return errors.Wrap(err, "failed to materialize labels values")
			}
			*v = values
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	for cIdx, values := range colsMap {
		labelName := strings.TrimPrefix(m.shard.f.Schema().Columns()[cIdx][0], "labels.")
		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			results[i] = append(results[i], labels.Label{
				Name:  labelName,
				Value: util.YoloString(value.ByteArray()),
			})
		}
	}

	return results, nil
}

// Materialize materializes series from the parquet file
func (m *Materializer) NewMaterialize(ctx context.Context, rgi int, skipChunks bool, rr []search.RowRange) ([]prom_storage.Series, error) {
	rg := m.shard.buf.DynamicRowGroup(rgi)
	// Group rows by metric name and labels to create series
	seriesMap := make(map[uint64]*parquetSeries)

	// Read only the rows in the filtered row ranges (no additional filtering needed)
	for _, rowRange := range rr {
		reader := rg.DynamicRows()
		if err := reader.SeekToRow(rowRange.From()); err != nil {
			return nil, fmt.Errorf("failed to seek to row: %w", err)
		}
		rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, rowRange.Count())}
		n, err := reader.ReadRows(rowBuf)
		if err != nil {
			if err != io.EOF {
				return nil, fmt.Errorf("failed to read rows: %w", err)
			}
		}
		if n < int(rowRange.Count()) {
			return nil, fmt.Errorf("too few rows: %d", n)
		}
		columns := m.shard.f.Schema().Columns()
		builder := labels.NewBuilder(labels.EmptyLabels())
		for i := 0; i < int(rowRange.Count()); i++ {
			row := rowBuf.Get(i)
			_ = row
			builder.Reset(labels.EmptyLabels())
			var (
				ts  int64
				val float64
			)
			for j, v := range row.Row {
				if v.IsNull() {
					continue
				}
				col := columns[j][0]
				switch col {
				case frostdb.ColumnTimestamp:
					ts = v.Int64()
				case frostdb.ColumnValue:
					val = v.Double()
				default:
					lblName := strings.TrimPrefix(col, "labels.")
					builder.Set(lblName, string(v.ByteArray()))
				}
			}
			lbls := builder.Labels()
			seriesKey := lbls.Hash()
			series, exists := seriesMap[seriesKey]
			if !exists {
				// Create new series with metric name and labels
				series = &parquetSeries{
					labels:  lbls,
					samples: make([]cortexpb.Sample, 0),
				}
				seriesMap[seriesKey] = series
			}
			series.samples = append(series.samples, cortexpb.Sample{
				TimestampMs: ts,
				Value:       val,
			})
		}
		//for row := rowRange.from; row < rowRange.from+rowRange.count; row++ {
		//	// Read metric name
		//	if err := metricNamePages.SeekToRow(row); err != nil {
		//		continue // Skip this row if we can't read
		//	}
		//	metricNamePage, err := metricNamePages.ReadPage()
		//	if err != nil {
		//		continue
		//	}
		//	rowMetricName := m.readStringValue(metricNamePage, 0)
		//
		//	// Read timestamp
		//	if err := timestampPages.SeekToRow(row); err != nil {
		//		continue
		//	}
		//	timestampPage, err := timestampPages.ReadPage()
		//	if err != nil {
		//		continue
		//	}
		//	timestamp := m.readInt64Value(timestampPage, 0)
		//
		//	// Read labels
		//	if err := labelsPages.SeekToRow(row); err != nil {
		//		continue
		//	}
		//	labelsPage, err := labelsPages.ReadPage()
		//	if err != nil {
		//		continue
		//	}
		//	labelsJSON := m.readBytesValue(labelsPage, 0)
		//
		//	// Parse labels
		//	var lbls labels.Labels
		//	if len(labelsJSON) > 0 {
		//		if err := lbls.UnmarshalJSON(labelsJSON); err != nil {
		//			continue // Skip this row if we can't parse labels
		//		}
		//	}
		//
		//	// Create series key (metric name + labels hash)
		//	seriesKey := fmt.Sprintf("%s:%d", rowMetricName, lbls.Hash())
		//
		//	// Get or create series
		//	series, exists := seriesMap[seriesKey]
		//	if !exists {
		//		// Create new series with metric name and labels
		//		seriesLabels := labels.NewBuilder(lbls)
		//		seriesLabels.Set(labels.MetricName, rowMetricName)
		//
		//		series = &parquetSeries{
		//			labels:  seriesLabels.Labels(),
		//			samples: make([]cortexpb.Sample, 0),
		//		}
		//		seriesMap[seriesKey] = series
		//	}
		//
		//	// Read value only if not skipping chunks
		//	if !skipChunks {
		//		if err := valuePages.SeekToRow(row); err != nil {
		//			continue
		//		}
		//		valuePage, err := valuePages.ReadPage()
		//		if err != nil {
		//			continue
		//		}
		//		value := m.readFloat64Value(valuePage, 0)
		//
		//		// Add sample to series
		//		series.samples = append(series.samples, cortexpb.Sample{
		//			TimestampMs: timestamp,
		//			Value:       value,
		//		})
		//	} else {
		//		// When skipping chunks, just add a placeholder sample with timestamp only
		//		series.samples = append(series.samples, cortexpb.Sample{
		//			TimestampMs: timestamp,
		//			Value:       0, // Placeholder value when skipping chunks
		//		})
		//	}
		//}
	}

	// Convert map to slice of series
	result := make([]prom_storage.Series, 0, len(seriesMap))
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

	sidx := 0
	if defs == nil {
		if s.syms == nil {
			s.syms = make([]int32, len(syms))
		} else {
			s.syms = slices.Grow(s.syms, len(syms))[:len(syms)]
		}
		for i := 0; i < len(syms); i++ {
			s.syms[i] = syms[sidx]
			sidx++
		}
	} else {
		if s.syms == nil {
			s.syms = make([]int32, len(defs))
		} else {
			s.syms = slices.Grow(s.syms, len(defs))[:len(defs)]
		}
		for i := range defs {
			if defs[i] == 1 {
				s.syms[i] = syms[sidx]
				sidx++
			} else {
				s.syms[i] = -1
			}
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
	colIndex, ok := m.m.columnIndices[frostdb.ColumnTimestamp]
	if !ok {
		return []RowRange{}, nil
	}

	cc := rg.ColumnChunks()[colIndex]

	// Look up the column to get the type for comparison
	col, ok := rg.Schema().Lookup(frostdb.ColumnTimestamp)
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
	if len(matchers) == 0 {
		return rr, nil
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
	colIndex, ok := m.m.columnIndices[""]
	if !ok {
		return []RowRange{}, nil
	}

	cc := rg.ColumnChunks()[colIndex]

	// Look up the column to get the type for comparison
	col, ok := rg.Schema().Lookup("")
	if !ok {
		return []RowRange{}, nil
	}
	expected := parquet.ValueOf(util2.YoloBuf(metricName))
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
			if comp(vi.At(), expected) != 0 {
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
