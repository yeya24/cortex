package tsdb

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"time"
)

type BadgerIndexCache struct {
	logger           log.Logger
	cache            *badger.DB
	maxItemSizeBytes uint64

	added    *prometheus.CounterVec
	overflow *prometheus.CounterVec

	commonMetrics *storecache.CommonMetrics
}

func NewBadgerIndexCache(logger log.Logger, commonMetrics *storecache.CommonMetrics, reg prometheus.Registerer) (*BadgerIndexCache, error) {
	badgerOptions := badger.
		DefaultOptions().
		WithLogger(&BadgerLogger{Logger: logger}).
		WithCompression(options.None).
		WithBlockCacheSize(0)
	bc, err := badger.Open(badgerOptions)
	if err != nil {
		return nil, err
	}

	if commonMetrics == nil {
		commonMetrics = storecache.NewCommonMetrics(reg)
	}

	c := &BadgerIndexCache{
		logger:        logger,
		cache:         bc,
		commonMetrics: commonMetrics,
	}
	c.added = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	c.added.WithLabelValues(storecache.CacheTypePostings)
	c.added.WithLabelValues(storecache.CacheTypeSeries)
	c.added.WithLabelValues(storecache.CacheTypeExpandedPostings)

	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.overflow = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	c.overflow.WithLabelValues(storecache.CacheTypePostings)
	c.overflow.WithLabelValues(storecache.CacheTypeSeries)
	c.overflow.WithLabelValues(storecache.CacheTypeExpandedPostings)

	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenancy.DefaultTenant)

	go c.retentionLoop()
	return c, nil
}

type BadgerLogger struct {
	Logger log.Logger
}

func (l *BadgerLogger) Errorf(f string, v ...interface{}) {
	level.Error(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}

func (l *BadgerLogger) Warningf(f string, v ...interface{}) {
	level.Warn(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}

func (l *BadgerLogger) Infof(f string, v ...interface{}) {
	level.Info(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}

func (l *BadgerLogger) Debugf(f string, v ...interface{}) {
	level.Debug(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}

func (c *BadgerIndexCache) set(typ string, key storecache.CacheKey, val []byte) {
	k := yoloBuf(key.String())
	size := uint64(len(k) + len(val))
	if size > c.maxItemSizeBytes {
		level.Info(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring..",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"cacheType", typ,
		)
		c.overflow.WithLabelValues(typ).Inc()
		return
	}

	c.cache.Update(func(txn *badger.Txn) error {
		return txn.Set(k, val)
	})
	c.added.WithLabelValues(typ).Inc()
}

func (c *BadgerIndexCache) retentionLoop() {
	tick := time.NewTicker()
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			lsm, vlog := c.cache.Size()
			c.cache.RunValueLogGC(0.5)
		}
	}
}

// StorePostings sets the postings identified by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *BadgerIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypePostings, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypePostings, storecache.CacheKey{Block: blockID.String(), Key: copyToKey(l)}, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *BadgerIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypePostings, tenant))
	defer timer.ObserveDuration()

	hits = map[labels.Label][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	c.cache.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			requests++
			var found bool
			item, err := txn.Get(yoloBuf(storecache.CacheKey{Block: blockIDKey, Key: storecache.CacheKeyPostings(key)}.String()))
			if err == nil {
				item.Value(func(val []byte) error {
					hits[key] = val
					hit++
					found = true
					return nil
				})
			}
			if !found {
				misses = append(misses, key)
			}
		}
		return nil
	})
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(hit))

	return hits, misses
}

// StoreExpandedPostings stores expanded postings for a set of label matchers.
func (c *BadgerIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypeExpandedPostings, storecache.CacheKey{Block: blockID.String(), Key: storecache.LabelMatchersToString(matchers)}, v)
}

// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
func (c *BadgerIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()

	if ctx.Err() != nil {
		return nil, false
	}
	var (
		found bool
		v     []byte
	)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Inc()
	c.cache.View(func(txn *badger.Txn) error {
		item, err := txn.Get(yoloBuf(storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeyExpandedPostings(storecache.LabelMatchersToString(matchers))}.String()))
		if err == nil {
			item.Value(func(val []byte) error {
				c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Inc()
				v = val
				found = true
				return nil
			})
		}
		return nil
	})
	return v, found
}

// StoreSeries sets the series identified by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *BadgerIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypeSeries, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypeSeries, storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeySeries(id)}, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *BadgerIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypeSeries, tenant))
	defer timer.ObserveDuration()

	hits = map[storage.SeriesRef][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	c.cache.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			requests++
			var found bool
			item, err := txn.Get(yoloBuf(storecache.CacheKey{Block: blockIDKey, Key: storecache.CacheKeySeries(id)}.String()))
			if err == nil {
				item.Value(func(val []byte) error {
					hits[id] = val
					hit++
					found = true
					return nil
				})
			}
			if !found {
				misses = append(misses, id)
			}
		}
		return nil
	})
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(hit))

	return hits, misses
}
