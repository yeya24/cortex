package tsdb

import (
	"context"
	"github.com/thanos-io/thanos/pkg/cache"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

type multiLevelIndexCache struct {
	caches []storecache.IndexCache
}

type multiLevelBucketCache struct {
	name   string
	caches []cache.Cache
}

func (m *multiLevelBucketCache) Store(data map[string][]byte, ttl time.Duration) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.Store(data, ttl)
		}()
	}
	wg.Wait()
}

func (m *multiLevelBucketCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	hits := make(map[string][]byte, len(keys))
	missed := keys
	nextMissed := make([]string, 0)
	//backfillMap := make(map[int]map[string][]byte)
	for _, c := range m.caches {
		res := c.Fetch(ctx, missed)
		for _, key := range missed {
			if data, ok := res[key]; ok {
				hits[key] = data
			} else {
				nextMissed = append(nextMissed, key)
			}
		}
		//
		//if i > 0 {
		//	backfillMap[i - 1] = res
		//}

		if len(nextMissed) == 0 {
			break
		}
		missed = nextMissed
		nextMissed = nextMissed[:0]
	}

	return hits
}

func (m *multiLevelBucketCache) Name() string {
	return m.name
}

func (m *multiLevelIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StorePostings(blockID, l, v)
		}()
	}
	wg.Wait()
}

func (m *multiLevelIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	misses = keys
	hits = map[labels.Label][]byte{}
	backfillMap := map[storecache.IndexCache][]map[labels.Label][]byte{}
	for i, c := range m.caches {
		backfillMap[c] = []map[labels.Label][]byte{}
		h, mi := c.FetchMultiPostings(ctx, blockID, misses)
		misses = mi

		for label, bytes := range h {
			hits[label] = bytes
		}

		if i > 0 {
			backfillMap[m.caches[i-1]] = append(backfillMap[m.caches[i-1]], h)
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		for cache, hit := range backfillMap {
			for _, values := range hit {
				for l, b := range values {
					cache.StorePostings(blockID, l, b)
				}
			}
		}
	}()

	return hits, misses
}

func (m *multiLevelIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StoreExpandedPostings(blockID, matchers, v)
		}()
	}
	wg.Wait()
}

func (m *multiLevelIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher) ([]byte, bool) {
	for i, c := range m.caches {
		if d, h := c.FetchExpandedPostings(ctx, blockID, matchers); h {
			if i > 0 {
				m.caches[i-1].StoreExpandedPostings(blockID, matchers, d)
			}
			return d, h
		}
	}

	return []byte{}, false
}

func (m *multiLevelIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StoreSeries(blockID, id, v)
		}()
	}
	wg.Wait()
}

func (m *multiLevelIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	misses = ids
	hits = map[storage.SeriesRef][]byte{}
	backfillMap := map[storecache.IndexCache][]map[storage.SeriesRef][]byte{}

	for i, c := range m.caches {
		backfillMap[c] = []map[storage.SeriesRef][]byte{}
		h, miss := c.FetchMultiSeries(ctx, blockID, misses)
		misses = miss

		for label, bytes := range h {
			hits[label] = bytes
		}

		if i > 0 && len(h) > 0 {
			backfillMap[m.caches[i-1]] = append(backfillMap[m.caches[i-1]], h)
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		for cache, hit := range backfillMap {
			for _, values := range hit {
				for m, b := range values {
					cache.StoreSeries(blockID, m, b)
				}
			}
		}
	}()

	return hits, misses
}

func newMultiLevelIndexCache(c ...storecache.IndexCache) storecache.IndexCache {
	if len(c) == 1 {
		return c[0]
	}
	return &multiLevelIndexCache{
		caches: c,
	}
}

func newMultiLevelBucketCache(name string, c ...cache.Cache) cache.Cache {
	if len(c) == 1 {
		return c[0]
	}
	return &multiLevelBucketCache{
		name:   name,
		caches: c,
	}
}
