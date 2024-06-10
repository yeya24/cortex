package cardinality

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/segmentio/parquet-go"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
)

const (
	workers = 8
)

type CardinalityExplorer struct {
	services.Service

	cfg               Config
	bucketClient      objstore.InstrumentedBucket
	queryBucketClient objstore.InstrumentedBucket
	logger            log.Logger
	reg               prometheus.Registerer
	cfgProvider       bucket.TenantConfigProvider

	lastSyncedCardinalityFiles []string
	lastSyncedQueryInfoFiles   []string
	cardinalityOverallMtx      sync.RWMutex
	metricCardinalityMtx       sync.RWMutex
	cardinalityOverall         []*TSDBStatusWithKey
	metricNameCardinalities    []*MetricNameCardinalitiesWithKey
	queryInfoMtx               sync.RWMutex
	queryInfoEntries           map[string]map[string]QueryInfoParquetEntry
	queryCount                 map[string]int
}

type TSDBStatusWithKey struct {
	*TSDBStatus
	key string
}

type MetricNameCardinalitiesWithKey struct {
	cardinalities *MetricNameCardinalities
	key           string
}

type Config struct {
	EnabledTenant string        `yaml:"enabled_tenant"`
	SyncInterval  time.Duration `yaml:"sync_interval"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.EnabledTenant, "enabled-tenant", "136742829649_ws-e312cc13-0ea0-4486-9f5f-8769e4023140", "enabled tenant for cardinality query")
	f.DurationVar(&cfg.SyncInterval, "sync-interval", time.Hour, "sync interval")
}

func NewCardinalityExplorer(cfg Config, bucketCfg bucket.Config, cfgProvider bucket.TenantConfigProvider, log log.Logger, reg prometheus.Registerer) (*CardinalityExplorer, error) {
	c := &CardinalityExplorer{
		cfg:                     cfg,
		logger:                  log,
		reg:                     reg,
		cfgProvider:             cfgProvider,
		cardinalityOverall:      make([]*TSDBStatusWithKey, 0),
		metricNameCardinalities: make([]*MetricNameCardinalitiesWithKey, 0),
		queryInfoEntries:        make(map[string]map[string]QueryInfoParquetEntry),
		queryCount:              make(map[string]int),
	}
	var err error
	c.bucketClient, err = bucket.NewClient(context.Background(), bucketCfg, "cardinality", c.logger, c.reg)
	if err != nil {
		return nil, err
	}
	queryBucketCfg := bucket.Config{
		Backend: "s3",
		S3: s3.Config{
			Endpoint:         "s3-us-west-2.amazonaws.com",
			Region:           "us-west-2",
			BucketName:       "amp-queryinfo-148585390640",
			BucketLookupType: s3.BucketAutoLookup,
			SignatureVersion: s3.SignatureVersionV4,
		},
	}
	c.queryBucketClient, err = bucket.NewClient(context.Background(), queryBucketCfg, "queryinfo", c.logger, c.reg)

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c, nil
}

func (c *CardinalityExplorer) starting(ctx context.Context) error {
	if err := c.syncCardinalityFiles(ctx); err != nil {
		return err
	}
	return c.syncQueryInfoFiles(ctx)
}

func (c *CardinalityExplorer) running(ctx context.Context) error {
	// Apply a jitter to the sync frequency in order to increase the probability
	// of hitting the shared cache (if any).
	syncTicker := time.NewTicker(util.DurationWithJitter(c.cfg.SyncInterval, 0.2))
	defer syncTicker.Stop()

	for {
		select {
		case <-syncTicker.C:
			if err := c.syncCardinalityFiles(ctx); err != nil {
				return err
			}
			if err := c.syncQueryInfoFiles(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *CardinalityExplorer) stopping(_ error) error {
	return nil
}

func (c *CardinalityExplorer) syncCardinalityFiles(ctx context.Context) error {
	bkt := bucket.NewUserBucketClient(c.cfg.EnabledTenant, c.bucketClient, c.cfgProvider)

	lastSyncedSet := make(map[string]struct{})
	for _, f := range c.lastSyncedCardinalityFiles {
		lastSyncedSet[f] = struct{}{}
	}

	synced := c.lastSyncedCardinalityFiles
	type key struct {
		path    string
		dateStr string
		file    string
	}
	var eg errgroup.Group
	fileChan := make(chan *key)

	for i := 0; i < 1; i++ {
		eg.Go(func() error {
			for p := range fileChan {
				reader, err := bkt.Get(ctx, p.path)
				if err != nil {
					return err
				}

				obj, err := io.ReadAll(reader)
				if err != nil {
					return err
				}

				if strings.Contains(p.file, "metric-cardinalities") {
					var m MetricNameCardinalities
					if err = json.Unmarshal(obj, &m); err != nil {
						return err
					}

					c.metricCardinalityMtx.Lock()
					c.metricNameCardinalities = append(c.metricNameCardinalities, &MetricNameCardinalitiesWithKey{cardinalities: &m, key: p.dateStr})
					c.metricCardinalityMtx.Unlock()
				} else if strings.Contains(p.file, "overview") {
					var m TSDBStatus
					if err = json.Unmarshal(obj, &m); err != nil {
						return err
					}

					c.cardinalityOverallMtx.Lock()
					c.cardinalityOverall = append(c.cardinalityOverall, &TSDBStatusWithKey{TSDBStatus: &m, key: p.dateStr})
					c.cardinalityOverallMtx.Unlock()
				}
			}
			return nil
		})
	}

	if err := bkt.Iter(ctx, "cardinality", func(s string) error {
		if !strings.HasSuffix(s, "json") {
			return nil
		}
		if _, ok := lastSyncedSet[s]; ok {
			return nil
		}

		// Check date str

		synced = append(synced, s)
		parts := strings.Split(s, "/")
		dateStr, file := parts[1], parts[len(parts)-1]

		select {
		case <-ctx.Done():
		case fileChan <- &key{path: s, file: file, dateStr: dateStr}:
		}
		return nil
	}, objstore.WithRecursiveIter); err != nil {
		return err
	}

	close(fileChan)
	if err := eg.Wait(); err != nil {
		return err
	}

	c.lastSyncedCardinalityFiles = synced
	return nil
}

func (c *CardinalityExplorer) syncQueryInfoFiles(ctx context.Context) error {
	bkt := bucket.NewUserBucketClient(c.cfg.EnabledTenant, c.queryBucketClient, c.cfgProvider)
	lastSyncedSet := make(map[string]struct{})
	for _, f := range c.lastSyncedQueryInfoFiles {
		lastSyncedSet[f] = struct{}{}
	}

	synced := make([]string, 0)
	type key struct {
		path    string
		dateStr string
		file    string
	}
	var eg errgroup.Group
	fileChan := make(chan *key)

	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for p := range fileChan {
				reader, err := bkt.Get(ctx, p.path)
				if err != nil {
					return err
				}

				obj, err := io.ReadAll(reader)
				if err != nil {
					return err
				}

				if err := os.WriteFile(p.file, obj, os.ModePerm); err != nil {
					return err
				}

				rows, err := parquet.ReadFile[QueryInfoParquetEntry](p.file)
				if err != nil {
					return err
				}
				c.queryInfoMtx.Lock()
				for _, row := range rows {
					if _, ok := c.queryInfoEntries[p.dateStr]; !ok {
						c.queryInfoEntries[p.dateStr] = make(map[string]QueryInfoParquetEntry)
					}
					c.queryInfoEntries[p.dateStr][row.MetricName] = row
					c.queryCount[p.dateStr] += row.Count
				}
				c.queryInfoMtx.Unlock()
			}
			return nil
		})
	}

	if err := bkt.Iter(ctx, "", func(s string) error {
		if !strings.HasSuffix(s, "parquet") {
			return nil
		}
		if _, ok := lastSyncedSet[s]; ok {
			return nil
		}

		// Check date str

		synced = append(synced, s)
		parts := strings.Split(s, "/")
		dateStr, file := parts[0], parts[len(parts)-1]

		select {
		case <-ctx.Done():
		case fileChan <- &key{path: s, file: file, dateStr: dateStr}:
		}
		return nil
	}, objstore.WithRecursiveIter); err != nil {
		return err
	}

	close(fileChan)
	if err := eg.Wait(); err != nil {
		return err
	}

	c.lastSyncedQueryInfoFiles = synced
	return nil
}

type QueryInfoParquetEntry struct {
	Workspace  string             `parquet:"workspace,dict"`
	MetricName string             `parquet:"metric_name,dict"`
	Count      int                `parquet:"count,dict"`
	TotalCount int                `parquet:"total_count,dict"`
	Labels     []LabelCardinality `parquet:"labels"`
}

type LabelCardinality struct {
	Label string `parquet:"label,dict"`
	Count int    `parquet:"count,dict"`
}

type CardinalityEntry struct {
	Name       string
	Count      uint64
	Percentage string
}

type TSDBStatusResponse struct {
	TotalSeries                 uint64             `json:"total_series"`
	TotalLabelValuePairs        uint64             `json:"total_label_value_pairs"`
	TotalQueryExecutions        uint64             `json:"total_query_executions"`
	QueryExecutionsByMetricName []CardinalityEntry `json:"query_executions_by_metric_name"`
	SeriesCountByMetricName     []CardinalityEntry `json:"series_count_by_metric_name"`
	SeriesCountByLabelName      []CardinalityEntry `json:"series_count_by_label_name"`
	SeriesCountByLabelValuePair []CardinalityEntry `json:"series_count_by_label_value_pair"`
	LabelValueCountByLabelName  []TopHeapEntry     `json:"label_value_count_by_label_name"`
}

type TotalSeriesRange struct {
	Time        time.Time `json:"timestamp"`
	TotalSeries uint64    `json:"total_series"`
}

func (c *CardinalityExplorer) GetCardinalityOverviewRange(w http.ResponseWriter, r *http.Request) {
	c.cardinalityOverallMtx.RLock()
	defer c.cardinalityOverallMtx.RUnlock()
	res := make([]TotalSeriesRange, 0, len(c.cardinalityOverall))
	for _, overview := range c.cardinalityOverall {
		t, err := time.Parse(time.DateOnly, overview.key)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		res = append(res, TotalSeriesRange{
			TotalSeries: overview.TotalSeries,
			Time:        t,
		})
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Time.Before(res[j].Time)
	})
	out, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	w.WriteHeader(http.StatusOK)
}

func (c *CardinalityExplorer) GetMetricNameRange(w http.ResponseWriter, r *http.Request) {
	metric := r.FormValue("metric")
	if metric == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	c.metricCardinalityMtx.RLock()
	defer c.metricCardinalityMtx.RUnlock()

	res := make([]TotalSeriesRange, 0)
OUTER:
	for _, m := range c.metricNameCardinalities {
		for _, mm := range *m.cardinalities {
			if mm.Name == metric {
				t, err := time.Parse(time.DateOnly, m.key)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				res = append(res, TotalSeriesRange{
					TotalSeries: mm.TotalSeries,
					Time:        t,
				})
				continue OUTER
			}
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Time.Before(res[j].Time)
	})
	out, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	w.WriteHeader(http.StatusOK)
}

func (c *CardinalityExplorer) GetCardinalityOverview(w http.ResponseWriter, r *http.Request) {
	date := r.FormValue("date")
	limitStr := r.FormValue("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	c.cardinalityOverallMtx.RLock()
	defer c.cardinalityOverallMtx.RUnlock()

	var res TSDBStatusResponse
	for _, item := range c.cardinalityOverall {
		if item.key == date {
			res.TotalSeries = item.TotalSeries
			res.TotalLabelValuePairs = item.TotalLabelValuePairs
			if queryC, ok := c.queryCount[date]; ok {
				res.TotalQueryExecutions = uint64(queryC)
			}

			if queryMap, ok := c.queryInfoEntries[date]; ok {
				queryExecutionsByMetricName := newTopHeap(limit)
				for key, entry := range queryMap {
					queryExecutionsByMetricName.push(key, uint64(entry.Count))
				}
				rr := queryExecutionsByMetricName.getSortedResult()
				res.QueryExecutionsByMetricName = make([]CardinalityEntry, len(rr))
				for i, qq := range rr {
					res.QueryExecutionsByMetricName[i] = CardinalityEntry{
						Name:  qq.Name,
						Count: qq.Count,
					}
					if res.TotalQueryExecutions > 0 {
						res.QueryExecutionsByMetricName[i].Percentage = fmt.Sprintf("%.2f%%", 100*float64(qq.Count)/float64(res.TotalQueryExecutions))
					} else {
						res.QueryExecutionsByMetricName[i].Percentage = "0%"
					}
				}
			}

			l := limit
			if len(item.SeriesCountByMetricName) < limit {
				l = len(item.SeriesCountByMetricName)
			}
			res.SeriesCountByMetricName = make([]CardinalityEntry, 0, l)
			for i := 0; i < l; i++ {
				res.SeriesCountByMetricName = append(res.SeriesCountByMetricName, CardinalityEntry{
					Name:       item.SeriesCountByMetricName[i].Name,
					Count:      item.SeriesCountByMetricName[i].Count,
					Percentage: fmt.Sprintf("%.2f%%", 100*float64(item.SeriesCountByMetricName[i].Count)/float64(item.TotalSeries)),
				})
			}

			l = limit
			if len(item.SeriesCountByLabelName) < limit {
				l = len(item.SeriesCountByLabelName)
			}
			res.SeriesCountByLabelName = make([]CardinalityEntry, 0, l)
			for i := 0; i < l; i++ {
				res.SeriesCountByLabelName = append(res.SeriesCountByLabelName, CardinalityEntry{
					Name:       item.SeriesCountByLabelName[i].Name,
					Count:      item.SeriesCountByLabelName[i].Count,
					Percentage: fmt.Sprintf("%.2f%%", 100*float64(item.SeriesCountByLabelName[i].Count)/float64(item.TotalSeries)),
				})
			}

			l = limit
			if len(item.SeriesCountByLabelValuePair) < limit {
				l = len(item.SeriesCountByLabelName)
			}
			res.SeriesCountByLabelValuePair = make([]CardinalityEntry, 0, l)
			for i := 0; i < l; i++ {
				res.SeriesCountByLabelValuePair = append(res.SeriesCountByLabelValuePair, CardinalityEntry{
					Name:       item.SeriesCountByLabelValuePair[i].Name,
					Count:      item.SeriesCountByLabelValuePair[i].Count,
					Percentage: fmt.Sprintf("%.2f%%", 100*float64(item.SeriesCountByLabelValuePair[i].Count)/float64(item.TotalSeries)),
				})
			}

			l = limit
			if len(item.LabelValueCountByLabelName) < limit {
				l = len(item.LabelValueCountByLabelName)
			}
			for i := 0; i < l; i++ {
				res.LabelValueCountByLabelName = append(res.LabelValueCountByLabelName, TopHeapEntry{
					Name:  item.LabelValueCountByLabelName[i].Name,
					Count: item.LabelValueCountByLabelName[i].Count,
				})
			}
		}
	}
	out, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	w.WriteHeader(http.StatusOK)
}

type MetricCardinalityResponse struct {
	Name        string `json:"name"`
	TotalSeries uint64 `json:"total_series"`
	// Cannot sum. Use Max() for it.
	TotalLabelValuePairs        uint64                      `json:"total_label_value_pairs"`
	AllLabels                   []LabelNameQueryCardinality `json:"labels"`
	SeriesCountByLabelValuePair []TopHeapEntry              `json:"series_count_by_label_value_pair"`
	QueryExecutions             int                         `json:"query_executions"`
}

type LabelNameQueryCardinality struct {
	TotalSeries     uint64 `json:"total_series"`
	Percentage      string `json:"percentage"`
	QueryPercentage string `json:"query_percentage"`
	QueryExecutions uint64 `json:"query_executions"`
	LabelValueCount uint64 `json:"label_value_count"`
	Name            string `json:"name"`
	// Record top 5 label value cardinality.
	LabelValueCardinality []TopHeapEntry `json:"label_value_cardinality"`
}

func (c *CardinalityExplorer) GetMetricNameCardinality(w http.ResponseWriter, r *http.Request) {
	date := r.FormValue("date")
	limitStr := r.FormValue("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	metric := r.FormValue("metric")

	c.metricCardinalityMtx.RLock()
	c.queryInfoMtx.RLock()
	defer c.queryInfoMtx.RUnlock()
	defer c.metricCardinalityMtx.RUnlock()

	queryMap := c.queryInfoEntries[date]
	var res MetricCardinalityResponse
OUTER:
	for _, item := range c.metricNameCardinalities {
		if item.key == date {
			for _, cardinality := range *item.cardinalities {
				if cardinality.Name == metric {
					res.Name = cardinality.Name
					res.TotalSeries = cardinality.TotalSeries
					res.TotalLabelValuePairs = cardinality.TotalLabelValuePairs

					l := limit
					if len(cardinality.SeriesCountByLabelValuePair) < limit {
						l = len(cardinality.SeriesCountByLabelValuePair)
					}
					for i := 0; i < l; i++ {
						res.SeriesCountByLabelValuePair = append(res.SeriesCountByLabelValuePair, TopHeapEntry{
							Name:  cardinality.SeriesCountByLabelValuePair[i].Name,
							Count: cardinality.SeriesCountByLabelValuePair[i].Count,
						})
					}

					var (
						entry QueryInfoParquetEntry
						ok    bool
					)
					if queryMap != nil {
						entry, ok = queryMap[metric]
						if ok {
							res.QueryExecutions = entry.Count
						}
					}
					res.AllLabels = make([]LabelNameQueryCardinality, len(cardinality.AllLabels))
					for i, label := range cardinality.AllLabels {
						res.AllLabels[i] = LabelNameQueryCardinality{
							TotalSeries:           label.TotalSeries,
							LabelValueCount:       label.LabelValueCount,
							Name:                  label.Name,
							LabelValueCardinality: label.LabelValueCardinality,
							Percentage:            fmt.Sprintf("%.2f%%", 100*float64(label.TotalSeries)/float64(cardinality.TotalSeries)),
						}
						if ok {
							for _, lbl := range entry.Labels {
								if lbl.Label == label.Name {
									res.AllLabels[i].QueryExecutions = uint64(lbl.Count)
									if res.QueryExecutions > 0 && lbl.Count > 0 {
										res.AllLabels[i].QueryPercentage = fmt.Sprintf("%.2f%%", 100*float64(lbl.Count)/float64(res.QueryExecutions))
									} else {
										res.AllLabels[i].QueryPercentage = "0%"
									}
								}
							}
						} else {
							res.AllLabels[i].QueryPercentage = "0%"
						}
					}

					sort.Slice(res.AllLabels, func(i, j int) bool {
						if res.AllLabels[i].TotalSeries == res.AllLabels[j].TotalSeries {
							return res.AllLabels[i].QueryExecutions > res.AllLabels[j].QueryExecutions
						}
						return res.AllLabels[i].TotalSeries > res.AllLabels[j].TotalSeries
					})
					break OUTER
				}
			}
		}
	}
	out, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	w.WriteHeader(http.StatusOK)
}

type MetricNameLabelCadinalityResponse struct {
}

func (c *CardinalityExplorer) GetMetricNameLabelCardinality(w http.ResponseWriter, r *http.Request) {
	date := r.FormValue("date")
	metric := r.FormValue("metric")
	label := r.FormValue("label")

	c.metricCardinalityMtx.RLock()
	c.queryInfoMtx.RLock()
	defer c.queryInfoMtx.RUnlock()
	defer c.metricCardinalityMtx.RUnlock()

	var res []TopHeapEntry
OUTER:
	for _, item := range c.metricNameCardinalities {
		if item.key == date {
			for _, cardinality := range *item.cardinalities {
				if cardinality.Name == metric {
					for _, lbl := range cardinality.AllLabels {
						if lbl.Name == label {
							res = lbl.LabelValueCardinality
							break OUTER
						}
					}
				}
			}
		}
	}
	out, err := json.Marshal(res)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	w.WriteHeader(http.StatusOK)
}
