package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Config struct {
	Date   string
	Tenant string
	DryRun bool
}

// RegisterFlags does what it says.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Date, "date", "", "Date")
	f.StringVar(&cfg.Tenant, "tenant", "136742829649_ws-e312cc13-0ea0-4486-9f5f-8769e4023140", "Tenant")
	f.BoolVar(&cfg.DryRun, "dryrun", true, "Don't actually do anything")
}

func main() {
	var cfg Config
	ctx := context.Background()
	flagext.RegisterFlags(&cfg)
	flag.Parse()
	logger := log.NewLogfmtLogger(os.Stdout)
	if err := run(ctx, logger, cfg); err != nil {
		level.Error(logger).Log("msg", "failed to run", "err", err)
		os.Exit(1)
	}
}

type mockTenantConfigProvider struct {
	s3SseType              string
	s3KmsKeyID             string
	s3KmsEncryptionContext string
}

func (m *mockTenantConfigProvider) S3SSEType(_ string) string {
	return m.s3SseType
}

func (m *mockTenantConfigProvider) S3SSEKMSKeyID(_ string) string {
	return m.s3KmsKeyID
}

func (m *mockTenantConfigProvider) S3SSEKMSEncryptionContext(_ string) string {
	return m.s3KmsEncryptionContext
}

func run(ctx context.Context, logger log.Logger, cfg Config) error {
	c, err := bucket.NewClient(ctx, bucket.Config{
		Backend: "s3",
		S3: s3.Config{
			Endpoint:         "s3-us-west-2.amazonaws.com",
			Region:           "us-west-2",
			BucketName:       "cortex-block-storage-148585390640",
			BucketLookupType: s3.BucketAutoLookup,
			SignatureVersion: s3.SignatureVersionV4,
		},
	}, "cardinality", logger, prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	cfgProvider := &mockTenantConfigProvider{}
	tenant := cfg.Tenant
	bucketIndex, err := bucketindex.ReadIndex(ctx, c, tenant, cfgProvider, logger)
	if err != nil {
		return err
	}
	c = bucket.NewUserBucketClient(tenant, c, cfgProvider)
	dateStr := cfg.Date
	dateT, _ := time.Parse(time.DateOnly, dateStr)
	minT := time.Date(dateT.Year(), dateT.Month(), dateT.Day(), 0, 0, 0, 0, time.UTC)
	maxT := minT.Add(time.Hour * 24)

	blockIDs := make([]ulid.ULID, 0)
	deletedBlockSet := make(map[string]struct{})
	for _, id := range bucketIndex.BlockDeletionMarks.GetULIDs() {
		deletedBlockSet[id.String()] = struct{}{}
	}
	for _, block := range bucketIndex.Blocks {
		if block.MinTime >= util.TimeToMillis(minT) && block.MaxTime <= util.TimeToMillis(maxT) {
			if _, ok := deletedBlockSet[block.ID.String()]; ok {
				continue
			}
			fmt.Println(block.ID.String(), block.MinTime, block.MaxTime)
			blockIDs = append(blockIDs, block.ID)
		}
	}

	if cfg.DryRun {
		return nil
	}

	eg := errgroup.Group{}
	eg.SetLimit(8)
	cacheDir := "data"
	for _, block := range blockIDs {
		b := block
		p := filepath.Join(cacheDir, b.String())
		if _, err := os.Stat(p); err != nil {
			if err := os.MkdirAll(p, os.ModePerm); err != nil {
				return err
			}
		}
		indexPath := filepath.Join(p, "index")
		if _, err := os.Stat(indexPath); err != nil {
			eg.Go(func() error {
				level.Info(logger).Log("msg", "start downloading index file", "path", indexPath)
				if err := objstore.DownloadFile(ctx, logger, c, filepath.Join(b.String(), "index"), p); err != nil {
					return err
				}
				return nil
			})
		} else {
			level.Info(logger).Log("msg", "index file already exists", "path", indexPath)
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	eg.SetLimit(4)
	for _, block := range blockIDs {
		b := block
		overviewPath := filepath.Join(cacheDir, b.String(), "overview.json")
		if _, err := os.Stat(overviewPath); err != nil {
			eg.Go(func() error {
				level.Info(logger).Log("msg", "start running cardinality analysis", "block", b.String())
				if err := Cardinality(ctx, "data", b.String(), 500, logger); err != nil {
					return err
				}
				return nil
			})
		} else {
			level.Info(logger).Log("msg", "overview file already exists", "path", overviewPath)
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	start := time.Now()
	overviews := make([]string, len(blockIDs))
	metricCardinalities := make([]string, len(blockIDs))
	for i, block := range blockIDs {
		b := block
		overviewPath := filepath.Join(cacheDir, b.String(), "overview.json")
		metricCardinalityPath := filepath.Join(cacheDir, b.String(), "metric-cardinalities.json")
		overviews[i] = overviewPath
		metricCardinalities[i] = metricCardinalityPath
	}

	f, err := os.OpenFile(overviews[0], os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	out, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	var mergedOverview TSDBStatus
	if err := json.Unmarshal(out, &mergedOverview); err != nil {
		return err
	}
	f.Close()

	for i := 1; i < len(overviews); i++ {
		f, err := os.OpenFile(overviews[i], os.O_RDONLY, 0600)
		if err != nil {
			return err
		}
		out, err = io.ReadAll(f)
		if err != nil {
			return err
		}
		var o TSDBStatus
		if err := json.Unmarshal(out, &o); err != nil {
			return err
		}

		mergedOverview.merge(o)
		f.Close()
	}

	if _, err := os.Stat(dateStr); err != nil {
		if err := os.Mkdir(dateStr, os.ModePerm); err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(dateStr, "overview.json"), os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return err
		}
		out, err := json.Marshal(mergedOverview)
		if err != nil {
			return err
		}
		if _, err := f.Write(out); err != nil {
			return err
		}
		f.Close()
	}
	level.Info(logger).Log("msg", "finished merging and writing overview.json", "duration", time.Since(start))

	start = time.Now()
	f, err = os.OpenFile(metricCardinalities[0], os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	out, err = io.ReadAll(f)
	if err != nil {
		return err
	}
	var mergedMetricCardinalities MetricNameCardinalities
	if err := json.Unmarshal(out, &mergedMetricCardinalities); err != nil {
		return err
	}
	f.Close()

	for i := 1; i < len(metricCardinalities); i++ {
		f, err := os.OpenFile(metricCardinalities[i], os.O_RDONLY, 0600)
		if err != nil {
			return err
		}
		out, err = io.ReadAll(f)
		if err != nil {
			return err
		}
		var o MetricNameCardinalities
		if err := json.Unmarshal(out, &o); err != nil {
			return err
		}

		mergedMetricCardinalities = mergedMetricCardinalities.merge(o)
		f.Close()
	}

	if _, err := os.Stat(dateStr); err != nil {
		if err := os.Mkdir(dateStr, os.ModePerm); err != nil {
			return err
		}
	}

	f, err = os.OpenFile(filepath.Join(dateStr, "metric-cardinalities.json"), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	out, err = json.Marshal(mergedMetricCardinalities)
	if err != nil {
		return err
	}
	if _, err := f.Write(out); err != nil {
		return err
	}
	f.Close()
	level.Info(logger).Log("msg", "finished merging and writing metrics cardinality", "duration", time.Since(start))

	return nil
}

const (
	cardinalityFolderName = "cardinality"
)

func Cardinality(ctx context.Context, path, blockID string, limit int, logger log.Logger) error {
	logger = log.With(logger, "block", blockID)
	var (
		err error
	)
	start := time.Now()

	ir, err := index.NewFileReader(filepath.Join(path, blockID, "index"))
	if err != nil {
		return err
	}
	defer ir.Close()

	stats := &TSDBStatus{}
	var totalSeries uint64
	seriesCountByMetricName := newTopHeap(limit)
	seriesCountByLabelName := newTopHeap(limit)
	seriesCountByLabelValuePairMap := make(map[string]uint64)
	labelValueCountByLabelName := newTopHeap(limit)

	allLabelNames, err := ir.LabelNames(ctx)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "all label names", "count", len(allLabelNames))

	allMetricNames, err := ir.LabelValues(ctx, labels.MetricName)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "all metric names", "count", len(allMetricNames))

	var (
		p index.Postings
	)
	p, err = ir.Postings(ctx, "", "") // The special all key.
	if err != nil {
		return err
	}

	builder := labels.ScratchBuilder{}
	for p.Next() {
		if err = ir.Series(p.At(), &builder, nil); err != nil {
			return err
		}
		totalSeries++
		builder.Labels().Range(func(lbl labels.Label) {
			key := lbl.Name + "=" + lbl.Value
			seriesCountByLabelValuePairMap[key]++
		})
	}
	if p.Err() != nil {
		return p.Err()
	}
	level.Info(logger).Log("msg", "all metric names", "count", len(allMetricNames))

	for _, n := range allLabelNames {
		values, err := ir.LabelValues(ctx, n)
		if err != nil {
			return err
		}
		labelValueCountByLabelName.push(n, uint64(len(values)))
		postings, err := ir.Postings(ctx, n, values...)
		if err != nil {
			return err
		}
		count := uint64(0)
		for postings.Next() {
			count++
		}
		if err := postings.Err(); err != nil {
			return err
		}
		seriesCountByLabelName.push(n, count)
	}
	level.Info(logger).Log("msg", "finished checking series count per label name")

	metricCardinalities := make([]*MetricNameCardinality, len(allMetricNames))
	for i, n := range allMetricNames {
		metricSeriesCountByLabelNameMap := make(map[string]uint64)
		metricSeriesCountByLabelValuePairMap := make(map[string]uint64)
		metricLabelValueCountByLabelName := newTopHeap(limit)
		labelValueSet := make(map[string]map[string]uint64)
		metricCardinalities[i] = &MetricNameCardinality{Name: n}

		postings, err := ir.Postings(ctx, labels.MetricName, n)
		if err != nil {
			return err
		}
		count := uint64(0)
		for postings.Next() {
			if err = ir.Series(postings.At(), &builder, nil); err != nil {
				return err
			}
			count++

			builder.Labels().Range(func(lbl labels.Label) {
				key := lbl.Name + "=" + lbl.Value
				metricSeriesCountByLabelNameMap[lbl.Name]++
				metricSeriesCountByLabelValuePairMap[key]++

				if _, ok := labelValueSet[lbl.Name]; !ok {
					labelValueSet[lbl.Name] = make(map[string]uint64)
				}
				values := labelValueSet[lbl.Name]
				values[lbl.Value]++
			})
		}
		if postings.Err() != nil {
			return err
		}
		seriesCountByMetricName.push(n, count)
		metricCardinalities[i].TotalSeries = count
		metricCardinalities[i].TotalLabelValuePairs = uint64(len(metricSeriesCountByLabelValuePairMap))
		metricCardinalities[i].AllLabels = make([]LabelNameCardinality, 0, len(labelValueSet))
		for k, v := range labelValueSet {
			h := newTopHeap(5)
			for value, c := range v {
				h.push(value, c)
			}
			lnc := LabelNameCardinality{
				Name:                  k,
				LabelValueCount:       uint64(len(v)),
				LabelValueCardinality: h.getSortedResult(),
				TotalSeries:           metricSeriesCountByLabelNameMap[k],
			}
			metricLabelValueCountByLabelName.push(k, uint64(len(v)))
			metricCardinalities[i].AllLabels = append(metricCardinalities[i].AllLabels, lnc)
		}
		allLabels := metricCardinalities[i].AllLabels
		sort.Slice(allLabels, func(i, j int) bool {
			if allLabels[i].TotalSeries == allLabels[j].TotalSeries {
				if allLabels[i].LabelValueCount == allLabels[j].LabelValueCount {
					return strings.Compare(allLabels[i].Name, allLabels[j].Name) < 0
				} else {
					return allLabels[i].LabelValueCount < allLabels[j].LabelValueCount
				}
			}
			return allLabels[i].TotalSeries > allLabels[j].TotalSeries
		})
		metricSeriesCountByLabelValuePairHeap := newTopHeap(limit)
		for k, v := range metricSeriesCountByLabelValuePairMap {
			metricSeriesCountByLabelValuePairHeap.push(k, v)
		}
		metricCardinalities[i].SeriesCountByLabelValuePair = metricSeriesCountByLabelValuePairHeap.getSortedResult()
	}

	sort.Slice(metricCardinalities, func(i, j int) bool {
		if metricCardinalities[i].TotalSeries == metricCardinalities[j].TotalSeries {
			if metricCardinalities[i].TotalLabelValuePairs == metricCardinalities[j].TotalLabelValuePairs {
				return strings.Compare(metricCardinalities[i].Name, metricCardinalities[j].Name) < 0
			} else {
				return metricCardinalities[i].TotalLabelValuePairs > metricCardinalities[j].TotalLabelValuePairs
			}
		}
		return metricCardinalities[i].TotalSeries > metricCardinalities[j].TotalSeries
	})
	output, err := json.Marshal(metricCardinalities)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(path, blockID, "metric-cardinalities.json"), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	if _, err := f.Write(output); err != nil {
		return err
	}
	f.Close()
	//if err := bucket.Upload(ctx, filepath.Join(cardinalityFolderName, dateStr, fmt.Sprintf("metric-cardinalities-%s.json", blockID)), bytes.NewReader(output)); err != nil {
	//	return err
	//}

	stats.TotalSeries = totalSeries
	stats.SeriesCountByLabelName = seriesCountByLabelName.getSortedResult()
	stats.SeriesCountByMetricName = seriesCountByMetricName.getSortedResult()
	seriesCountByLabelValuePairHeap := newTopHeap(limit)
	for k, v := range seriesCountByLabelValuePairMap {
		seriesCountByLabelValuePairHeap.push(k, v)
	}
	stats.SeriesCountByLabelValuePair = seriesCountByLabelValuePairHeap.getSortedResult()
	stats.TotalLabelValuePairs = uint64(len(seriesCountByLabelValuePairMap))
	stats.LabelValueCountByLabelName = labelValueCountByLabelName.getSortedResult()

	output, err = json.Marshal(stats)
	if err != nil {
		return err
	}

	f, err = os.OpenFile(filepath.Join(path, blockID, "overview.json"), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	if _, err := f.Write(output); err != nil {
		return err
	}
	f.Close()

	//if err := bucket.Upload(ctx, filepath.Join(cardinalityFolderName, dateStr, fmt.Sprintf("overall-%s.json", blockID)), bytes.NewReader(output)); err != nil {
	//	return err
	//}

	level.Info(logger).Log("msg", "finished uploading cardinality files", "duration", time.Since(start))
	return nil
}

type TSDBStatus struct {
	TotalSeries                 uint64         `json:"total_series"`
	TotalLabelValuePairs        uint64         `json:"total_label_value_pairs"`
	SeriesCountByMetricName     []TopHeapEntry `json:"series_count_by_metric_name"`
	SeriesCountByLabelName      []TopHeapEntry `json:"series_count_by_label_name"`
	SeriesCountByLabelValuePair []TopHeapEntry `json:"series_count_by_label_value_pair"`
	LabelValueCountByLabelName  []TopHeapEntry `json:"label_value_count_by_label_name"`
}

func mergeTopHeap(h1 []TopHeapEntry, h2 []TopHeapEntry, topN int) []TopHeapEntry {
	m := make(map[string]uint64)
	for _, item := range h1 {
		m[item.Name] = item.Count
	}
	for _, item := range h2 {
		if _, ok := m[item.Name]; ok {
			m[item.Name] += item.Count
		} else {
			m[item.Name] = item.Count
		}
	}
	h := newTopHeap(topN)
	for k, v := range m {
		h.push(k, v)
	}
	return h.getSortedResult()
}

func (t *TSDBStatus) merge(other TSDBStatus) {
	t.TotalSeries += other.TotalSeries
	if t.TotalLabelValuePairs < other.TotalLabelValuePairs {
		t.TotalLabelValuePairs = other.TotalLabelValuePairs
	}

	t.SeriesCountByLabelName = mergeTopHeap(t.SeriesCountByLabelName, other.SeriesCountByLabelName, 500)

	t.SeriesCountByMetricName = mergeTopHeap(t.SeriesCountByMetricName, other.SeriesCountByMetricName, 500)

	t.SeriesCountByLabelValuePair = mergeTopHeap(t.SeriesCountByLabelValuePair, other.SeriesCountByLabelValuePair, 500)

	labelValueCountByLabelNameMap := make(map[string]uint64)
	labelValueCountByLabelName := newTopHeap(500)
	for _, item := range t.LabelValueCountByLabelName {
		labelValueCountByLabelNameMap[item.Name] = item.Count
	}
	for _, item := range other.LabelValueCountByLabelName {
		if c, ok := labelValueCountByLabelNameMap[item.Name]; !ok || item.Count > c {
			labelValueCountByLabelNameMap[item.Name] = item.Count
		}
		labelValueCountByLabelName.push(item.Name, item.Count)
	}
	t.LabelValueCountByLabelName = labelValueCountByLabelName.getSortedResult()
}

type MetricNameCardinalities []MetricNameCardinality

func (m MetricNameCardinalities) merge(other MetricNameCardinalities) MetricNameCardinalities {
	mm := make(map[string]MetricNameCardinality)
	for _, item := range m {
		mm[item.Name] = item
	}
	for _, item := range other {
		if _, ok := mm[item.Name]; !ok {
			mm[item.Name] = item
		} else {
			mm[item.Name] = mm[item.Name].merge(item)
		}
	}
	mmc := make([]MetricNameCardinality, 0, len(mm))
	for _, item := range mm {
		mmc = append(mmc, item)
	}
	sort.Slice(mmc, func(i, j int) bool {
		if mmc[i].TotalSeries == mmc[j].TotalSeries {
			if mmc[i].TotalLabelValuePairs == mmc[j].TotalLabelValuePairs {
				return strings.Compare(mmc[i].Name, mmc[j].Name) < 0
			}
			return mmc[i].TotalLabelValuePairs > mmc[j].TotalLabelValuePairs
		}
		return mmc[i].TotalSeries > mmc[j].TotalSeries
	})
	return mmc
}

func (m MetricNameCardinality) merge(other MetricNameCardinality) MetricNameCardinality {
	if m.Name != other.Name {
		return MetricNameCardinality{}
	}
	m.TotalSeries += other.TotalSeries
	if m.TotalLabelValuePairs < other.TotalLabelValuePairs {
		m.TotalLabelValuePairs = other.TotalLabelValuePairs
	}
	lblsMap := make(map[string]LabelNameCardinality)
	for _, lbl := range m.AllLabels {
		lblsMap[lbl.Name] = lbl
	}
	for _, lbl := range other.AllLabels {
		if _, ok := lblsMap[lbl.Name]; !ok {
			lblsMap[lbl.Name] = lbl
		} else {
			lblsMap[lbl.Name] = lblsMap[lbl.Name].merge(lbl)
		}
	}
	lblsC := make([]LabelNameCardinality, 0, len(lblsMap))
	for _, lbl := range lblsMap {
		lblsC = append(lblsC, lbl)
	}
	sort.Slice(lblsC, func(i, j int) bool {
		if lblsC[i].TotalSeries == lblsC[j].TotalSeries {
			if lblsC[i].LabelValueCount == lblsC[j].LabelValueCount {
				return strings.Compare(lblsC[i].Name, lblsC[j].Name) < 0
			}
			return lblsC[i].LabelValueCount > lblsC[j].LabelValueCount
		}
		return lblsC[i].TotalSeries > lblsC[j].TotalSeries
	})
	m.AllLabels = lblsC

	m.SeriesCountByLabelValuePair = mergeTopHeap(m.SeriesCountByLabelValuePair, other.SeriesCountByLabelValuePair, 500)
	return m
}

func (m LabelNameCardinality) merge(other LabelNameCardinality) LabelNameCardinality {
	if m.Name != other.Name {
		return LabelNameCardinality{}
	}
	m.TotalSeries += other.TotalSeries
	if m.LabelValueCount < other.LabelValueCount {
		m.LabelValueCount = other.LabelValueCount
	}

	labelValueCardinalityMap := make(map[string]uint64)
	for _, item := range m.LabelValueCardinality {
		labelValueCardinalityMap[item.Name] = item.Count
	}
	for _, item := range other.LabelValueCardinality {
		if c, ok := labelValueCardinalityMap[item.Name]; !ok || item.Count > c {
			labelValueCardinalityMap[item.Name] = item.Count
		}
	}
	h := newTopHeap(5)
	for k, v := range labelValueCardinalityMap {
		h.push(k, v)
	}

	m.LabelValueCardinality = h.getSortedResult()
	return m
}

type MetricNameCardinality struct {
	Name        string `json:"name"`
	TotalSeries uint64 `json:"total_series"`
	// Cannot sum. Use Max() for it.
	TotalLabelValuePairs        uint64                 `json:"total_label_value_pairs"`
	AllLabels                   []LabelNameCardinality `json:"labels"`
	SeriesCountByLabelValuePair []TopHeapEntry         `json:"series_count_by_label_value_pair"`
}

type LabelNameCardinality struct {
	TotalSeries     uint64 `json:"total_series"`
	LabelValueCount uint64 `json:"label_value_count"`
	Name            string `json:"name"`
	// Record top 5 label value cardinality.
	LabelValueCardinality []TopHeapEntry `json:"label_value_cardinality"`
}

// topHeap maintains a heap of topHeapEntries with the maximum TopHeapEntry.n values.
type topHeap struct {
	topN int
	a    []TopHeapEntry
}

// newTopHeap returns topHeap for topN items.
func newTopHeap(topN int) *topHeap {
	return &topHeap{
		topN: topN,
	}
}

// TopHeapEntry represents an entry from `top heap` used in stats.
type TopHeapEntry struct {
	Name  string
	Count uint64
}

func (th *topHeap) push(name string, count uint64) {
	if count == 0 {
		return
	}
	if len(th.a) < th.topN {
		th.a = append(th.a, TopHeapEntry{
			Name:  name,
			Count: count,
		})
		heap.Fix(th, len(th.a)-1)
		return
	}
	if count <= th.a[0].Count {
		return
	}
	th.a[0] = TopHeapEntry{
		Name:  name,
		Count: count,
	}
	heap.Fix(th, 0)
}

func (th *topHeap) getSortedResult() []TopHeapEntry {
	result := append([]TopHeapEntry{}, th.a...)
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.Name < b.Name
	})
	return result
}

// heap.Interface implementation for topHeap.

func (th *topHeap) Len() int {
	return len(th.a)
}

func (th *topHeap) Less(i, j int) bool {
	a := th.a
	return a[i].Count < a[j].Count
}

func (th *topHeap) Swap(i, j int) {
	a := th.a
	a[j], a[i] = a[i], a[j]
}

func (th *topHeap) Push(_ interface{}) {
	panic(fmt.Errorf("BUG: Push shouldn't be called"))
}

func (th *topHeap) Pop() interface{} {
	panic(fmt.Errorf("BUG: Pop shouldn't be called"))
}
