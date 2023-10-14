package tsdb

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/outcaste-io/ristretto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"gopkg.in/yaml.v2"
	"reflect"
	"unsafe"
)

type cacheKey struct {
	block string
	key   interface{}

	compression string
}

const (
	cacheTypePostings         string = "Postings"
	cacheTypeExpandedPostings string = "ExpandedPostings"
	cacheTypeSeries           string = "Series"

	sliceHeaderSize = 16
)

type cacheKeyPostings labels.Label
type cacheKeyExpandedPostings string // We don't use []*labels.Matcher because it is not a hashable type so fail at inmemory cache.
type cacheKeySeries uint64

// Common metrics that should be used by all cache implementations.
type commonMetrics struct {
	requestTotal  *prometheus.CounterVec
	hitsTotal     *prometheus.CounterVec
	dataSizeBytes *prometheus.HistogramVec
	fetchLatency  *prometheus.HistogramVec
}

func newCommonMetrics(reg prometheus.Registerer) *commonMetrics {
	return &commonMetrics{
		requestTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_index_cache_requests_total",
			Help: "Total number of items requests to the cache.",
		}, []string{"item_type"}),
		hitsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_index_cache_hits_total",
			Help: "Total number of items requests to the cache that were a hit.",
		}, []string{"item_type"}),
		dataSizeBytes: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_store_index_cache_stored_data_size_bytes",
			Help: "Histogram to track item data size stored in index cache",
			Buckets: []float64{
				32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
			},
		}, []string{"item_type"}),
		fetchLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_index_cache_fetch_duration_seconds",
			Help:    "Histogram to track latency to fetch items from index cache",
			Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 10, 15, 20, 30, 45, 60, 90, 120},
		}, []string{"item_type"}),
	}
}

type InMemoryIndexCache struct {
	logger           log.Logger
	cache            *ristretto.Cache
	maxSizeBytes     uint64
	maxItemSizeBytes uint64

	commonMetrics *commonMetrics
	overflow      *prometheus.CounterVec
}

// parseInMemoryIndexCacheConfig unmarshals a buffer into a InMemoryIndexCacheConfig with default values.
func parseInMemoryIndexCacheConfig(conf []byte) (InMemoryIndexCacheConfig, error) {
	config := DefaultInMemoryIndexCacheConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return InMemoryIndexCacheConfig{}, err
	}

	return config, nil
}

// NewInMemoryIndexCacheWithConfig creates a new thread-safe LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func NewInMemoryIndexCacheWithConfig(logger log.Logger, commonMetrics *commonMetrics, reg prometheus.Registerer, config InMemoryIndexCacheConfig) (*InMemoryIndexCache, error) {
	if config.MaxSizeBytes > config.MaxSize {
		return nil, errors.Errorf("max item size (%v) cannot be bigger than overall cache size (%v)", config.MaxItemSize, config.MaxSize)
	}

	if commonMetrics == nil {
		commonMetrics = newCommonMetrics(reg)
	}

	c := &InMemoryIndexCache{
		commonMetrics:    commonMetrics,
		logger:           logger,
		maxSizeBytes:     uint64(config.MaxSize),
		maxItemSizeBytes: uint64(config.MaxItemSize),
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        int64(config.MaxSize) / 1024,
		MaxCost:            int64(config.MaxSize),
		BufferItems:        64,
		Metrics:            true,
		IgnoreInternalCost: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create inmemory cache")
	}
	c.cache = cache

	c.commonMetrics.requestTotal.WithLabelValues(cacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.requestTotal.WithLabelValues(cacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.requestTotal.WithLabelValues(cacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.commonMetrics.hitsTotal.WithLabelValues(cacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.hitsTotal.WithLabelValues(cacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.hitsTotal.WithLabelValues(cacheTypeExpandedPostings, tenancy.DefaultTenant)

	level.Info(logger).Log(
		"msg", "created in-memory index cache",
		"maxItemSizeBytes", c.maxItemSizeBytes,
		"maxSizeBytes", c.maxSizeBytes,
		"maxItems", "maxInt",
	)
	return c, nil
}

func (c *InMemoryIndexCache) set(typ string, key cacheKey, val []byte) {
	var size = sliceHeaderSize + uint64(len(val))
	if _, ok := c.cache.Get(key); ok {
		return
	}
	if size > c.maxItemSizeBytes {
		level.Debug(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring..",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"maxSizeBytes", c.maxSizeBytes,
			"itemSize", size,
			"cacheType", typ,
		)
		c.overflow.WithLabelValues(typ).Inc()
		return
	}

	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	v := make([]byte, len(val))
	copy(v, val)
	c.cache.Set(key, v, int64(size))
}

func (c *InMemoryIndexCache) get(typ string, key cacheKey, tenant string) ([]byte, bool) {
	c.commonMetrics.requestTotal.WithLabelValues(typ, tenant).Inc()

	v, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}
	c.commonMetrics.hitsTotal.WithLabelValues(typ, tenant).Inc()
	return v.([]byte), true
}

func copyString(s string) string {
	var b []byte
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
	h.Len = len(s)
	h.Cap = len(s)
	return string(b)
}

// copyToKey is required as underlying strings might be mmaped.
func copyToKey(l labels.Label) cacheKeyPostings {
	return cacheKeyPostings(labels.Label{Value: copyString(l.Value), Name: copyString(l.Name)})
}

// StorePostings sets the postings identified by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	c.commonMetrics.dataSizeBytes.WithLabelValues(cacheTypePostings, tenant).Observe(float64(len(v)))
	c.set(cacheTypePostings, cacheKey{block: blockID.String(), key: copyToKey(l)}, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *InMemoryIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(c.commonMetrics.fetchLatency.WithLabelValues(cacheTypePostings, tenant))
	defer timer.ObserveDuration()

	hits = map[labels.Label][]byte{}

	blockIDKey := blockID.String()
	for _, key := range keys {
		if ctx.Err() != nil {
			return hits, misses
		}
		if b, ok := c.get(cacheTypePostings, cacheKey{blockIDKey, cacheKeyPostings(key), ""}, tenant); ok {
			hits[key] = b
			continue
		}

		misses = append(misses, key)
	}

	return hits, misses
}

// StoreExpandedPostings stores expanded postings for a set of label matchers.
func (c *InMemoryIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	c.commonMetrics.dataSizeBytes.WithLabelValues(cacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	c.set(cacheTypeExpandedPostings, cacheKey{block: blockID.String(), key: cacheKeyExpandedPostings(labelMatchersToString(matchers))}, v)
}

// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
func (c *InMemoryIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.commonMetrics.fetchLatency.WithLabelValues(cacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()

	if ctx.Err() != nil {
		return nil, false
	}
	if b, ok := c.get(cacheTypeExpandedPostings, cacheKey{blockID.String(), cacheKeyExpandedPostings(labelMatchersToString(matchers)), ""}, tenant); ok {
		return b, true
	}
	return nil, false
}

// StoreSeries sets the series identified by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	c.commonMetrics.dataSizeBytes.WithLabelValues(cacheTypeSeries, tenant).Observe(float64(len(v)))
	c.set(cacheTypeSeries, cacheKey{blockID.String(), cacheKeySeries(id), ""}, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *InMemoryIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.commonMetrics.fetchLatency.WithLabelValues(cacheTypeSeries, tenant))
	defer timer.ObserveDuration()

	hits = map[storage.SeriesRef][]byte{}

	blockIDKey := blockID.String()
	for _, id := range ids {
		if ctx.Err() != nil {
			return hits, misses
		}
		if b, ok := c.get(cacheTypeSeries, cacheKey{blockIDKey, cacheKeySeries(id), ""}, tenant); ok {
			hits[id] = b
			continue
		}

		misses = append(misses, id)
	}

	return hits, misses
}
