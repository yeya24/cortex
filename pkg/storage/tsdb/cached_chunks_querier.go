package tsdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"
)

/*
	This file is basically a copy from https://github.com/prometheus/prometheus/blob/e2e01c1cffbfc4f26f5e9fe6138af87d7ff16122/tsdb/querier.go
	with the difference that the PostingsForMatchers function is called from the Postings Cache
*/

// headPostingsRefsContextKey is the context key for passing a pointer to a slice that will be
// filled with head block series refs (a copy of postings) when selectChunkSeriesSet runs for the head.
// Used by the ingester for ingestion-delay metrics. The value must be *[]storage.SeriesRef.
type headPostingsRefsContextKeyType struct{}

// HeadPostingsRefsContextKey is the context key for head postings refs output.
// Attach context.WithValue(ctx, HeadPostingsRefsContextKey, &refs) before ChunkQuerier.Select;
// if the query includes the head block, refs will be filled with a copy of the head postings refs.
var HeadPostingsRefsContextKey = &headPostingsRefsContextKeyType{}

// noopExpandedPostingsCache implements ExpandedPostingsCache by calling PostingsForMatchers on the index.
// Use when the expanded postings cache is disabled so the head block still goes through selectChunkSeriesSet
// and ingestion-delay tracking can capture head refs via context.
type noopExpandedPostingsCache struct{}

func (noopExpandedPostingsCache) PostingsForMatchers(ctx context.Context, _ ulid.ULID, ix prom_tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
	return prom_tsdb.PostingsForMatchers(ctx, ix, ms...)
}

func (noopExpandedPostingsCache) ExpireSeries(_ labels.Labels) {}

func (noopExpandedPostingsCache) PurgeExpiredItems() {}

func (noopExpandedPostingsCache) Clear() {}

func (noopExpandedPostingsCache) Size() int { return 0 }

// NoopExpandedPostingsCache is a cache that does not cache; it fetches postings from the index each time.
// Use with NewCachedBlockChunkQuerier when ExpandedPostingsCache is nil so ingestion-delay tracking still works.
var NoopExpandedPostingsCache ExpandedPostingsCache = noopExpandedPostingsCache{}

type blockBaseQuerier struct {
	blockID    ulid.ULID
	index      prom_tsdb.IndexReader
	chunks     prom_tsdb.ChunkReader
	tombstones tombstones.Reader

	closed bool

	mint, maxt int64
}

func newBlockBaseQuerier(b prom_tsdb.BlockReader, mint, maxt int64) (*blockBaseQuerier, error) {
	indexr, err := b.Index()
	if err != nil {
		return nil, fmt.Errorf("open index reader: %w", err)
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, fmt.Errorf("open chunk reader: %w", err)
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, fmt.Errorf("open tombstone reader: %w", err)
	}

	if tombsr == nil {
		tombsr = tombstones.NewMemTombstones()
	}
	return &blockBaseQuerier{
		blockID:    b.Meta().ULID,
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

func (q *blockBaseQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	res, err := q.index.SortedLabelValues(ctx, name, hints, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	res, err := q.index.LabelNames(ctx, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	errs := tsdb_errors.NewMulti(
		q.index.Close(),
		q.chunks.Close(),
		q.tombstones.Close(),
	)
	q.closed = true
	return errs.Err()
}

type cachedBlockChunkQuerier struct {
	*blockBaseQuerier

	cache ExpandedPostingsCache
}

func NewCachedBlockChunkQuerier(cache ExpandedPostingsCache, b prom_tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := newBlockBaseQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &cachedBlockChunkQuerier{blockBaseQuerier: q, cache: cache}, nil
}

func (q *cachedBlockChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	return selectChunkSeriesSet(ctx, sortSeries, hints, ms, q.blockID, q.index, q.chunks, q.tombstones, q.mint, q.maxt, q.cache)
}

func selectChunkSeriesSet(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms []*labels.Matcher,
	blockID ulid.ULID, indexReader prom_tsdb.IndexReader, chunks prom_tsdb.ChunkReader, tombstones tombstones.Reader, mint, maxt int64,
	cache ExpandedPostingsCache,
) storage.ChunkSeriesSet {
	disableTrimming := false
	sharded := hints != nil && hints.ShardCount > 0

	if hints != nil {
		mint = hints.Start
		maxt = hints.End
		disableTrimming = hints.DisableTrimming
	}
	p, err := cache.PostingsForMatchers(ctx, blockID, indexReader, ms...)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	if sharded {
		p = indexReader.ShardedPostings(p, hints.ShardIndex, hints.ShardCount)
	}
	if sortSeries {
		p = indexReader.SortedPostings(p)
	}

	// When this is the head block and the caller wants a copy of postings refs (for ingestion-delay metrics),
	// copy refs into the context-held slice and use list postings from that same slice for the series set.
	// ListPostings mutates only its slice header (list = list[1:]), so the slice in *out keeps all refs.
	if blockID == rangeHeadULID {
		if v := ctx.Value(HeadPostingsRefsContextKey); v != nil {
			if out, ok := v.(*[]storage.SeriesRef); ok && out != nil {
				refsCopy := make([]storage.SeriesRef, 0)
				for p.Next() {
					refsCopy = append(refsCopy, p.At())
				}
				if err := p.Err(); err != nil {
					return storage.ErrChunkSeriesSet(err)
				}
				*out = refsCopy
				p = index.NewListPostings(refsCopy)
			}
		}
	}

	return prom_tsdb.NewBlockChunkSeriesSet(blockID, indexReader, chunks, tombstones, p, mint, maxt, disableTrimming)
}
