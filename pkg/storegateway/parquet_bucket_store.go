package storegateway

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/parquetutil"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// parquetBlockMeta stores metadata about a parquet block
type parquetBlockMeta struct {
	id     ulid.ULID
	minT   int64
	maxT   int64
	shards int
}

type parquetBucketStore struct {
	logger      log.Logger
	bucket      objstore.Bucket
	limits      *validation.Overrides
	concurrency int

	chunksDecoder *schema.PrometheusParquetChunksDecoder

	matcherCache      storecache.MatchersCache
	parquetShardCache parquetutil.CacheInterface[parquet_storage.ParquetShard]

	// Synced blocks metadata
	blocksMu sync.RWMutex
	blocks   map[ulid.ULID]*parquetBlockMeta
}

func (p *parquetBucketStore) Close() error {
	p.parquetShardCache.Close()
	return p.bucket.Close()
}

func (p *parquetBucketStore) SyncBlocks(ctx context.Context) error {
	return p.syncBlocks(ctx)
}

func (p *parquetBucketStore) InitialSync(ctx context.Context) error {
	return p.syncBlocks(ctx)
}

func (p *parquetBucketStore) syncBlocks(ctx context.Context) error {
	spanLog, spanCtx := spanlogger.New(ctx, "parquetBucketStore.syncBlocks")
	defer spanLog.Finish()

	// Get user ID from context (injected by syncUsersBlocks)
	userID, err := users.TenantID(spanCtx)
	if err != nil {
		level.Warn(p.logger).Log("msg", "cannot get user ID from context for sync", "err", err)
		return nil
	}

	// Read bucket index
	idx, err := bucketindex.ReadIndex(spanCtx, p.bucket, userID, p.limits, p.logger)
	if err != nil {
		if errors.Is(err, bucketindex.ErrIndexNotFound) {
			level.Debug(p.logger).Log("msg", "bucket index not found", "user", userID)
			return nil
		}
		if errors.Is(err, bucketindex.ErrIndexCorrupted) {
			level.Error(p.logger).Log("msg", "corrupted bucket index", "user", userID, "err", err)
			return nil
		}
		return errors.Wrap(err, "read bucket index")
	}

	// Extract parquet blocks
	parquetBlocks := idx.ParquetBlocks()
	
	// Update blocks map
	p.blocksMu.Lock()
	defer p.blocksMu.Unlock()

	// Clear old blocks and rebuild
	p.blocks = make(map[ulid.ULID]*parquetBlockMeta, len(parquetBlocks))
	
	for _, b := range parquetBlocks {
		shards := 1 // default to 1 shard for backward compatibility
		if b.Parquet != nil && b.Parquet.Shards > 0 {
			shards = b.Parquet.Shards
		}
		
		p.blocks[b.ID] = &parquetBlockMeta{
			id:     b.ID,
			minT:   b.MinTime,
			maxT:   b.MaxTime,
			shards: shards,
		}
	}

	level.Info(p.logger).Log("msg", "synced parquet blocks", "user", userID, "blocks", len(p.blocks))
	return nil
}

func (p *parquetBucketStore) findParquetBlocks(ctx context.Context, blockMatchers []storepb.LabelMatcher) ([]*parquetBlock, error) {
	if len(blockMatchers) != 1 || blockMatchers[0].Type != storepb.LabelMatcher_RE || blockMatchers[0].Name != block.BlockIDLabel {
		return nil, status.Error(codes.InvalidArgument, "only one block matcher is supported")
	}

	blockIDs := strings.Split(blockMatchers[0].Value, "|")
	blocks := make([]*parquetBlock, 0, len(blockIDs)*2) // Estimate space for shards
	bucketOpener := parquet_storage.NewParquetBucketOpener(p.bucket)
	noopQuota := search.NewQuota(search.NoopQuotaLimitFunc(ctx))
	
	p.blocksMu.RLock()
	defer p.blocksMu.RUnlock()
	
	for _, blockIDStr := range blockIDs {
		blockID, err := ulid.Parse(blockIDStr)
		if err != nil {
			return nil, errors.Wrapf(err, "parse block ID: %s", blockIDStr)
		}
		
		// Get block metadata from synced blocks
		meta, exists := p.blocks[blockID]
		if !exists {
			level.Warn(p.logger).Log("msg", "block not found in synced metadata", "block", blockIDStr)
			// For backward compatibility, try opening shard 0
			block, err := p.newParquetBlock(ctx, blockIDStr, 0, bucketOpener, bucketOpener, p.chunksDecoder, noopQuota, noopQuota, noopQuota)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, block)
			continue
		}
		
		// Open all shards for this block
		for shardID := 0; shardID < meta.shards; shardID++ {
			block, err := p.newParquetBlock(ctx, blockIDStr, shardID, bucketOpener, bucketOpener, p.chunksDecoder, noopQuota, noopQuota, noopQuota)
			if err != nil {
				return nil, errors.Wrapf(err, "open block %s shard %d", blockIDStr, shardID)
			}
			blocks = append(blocks, block)
		}
	}

	return blocks, nil
}

// Series implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	spanLog, ctx := spanlogger.New(srv.Context(), "ParquetBucketStore.Series")
	defer spanLog.Finish()

	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	resHints := &hintspb.SeriesResponseHints{}
	var anyHints *types.Any

	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.SeriesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}
	ctx = injectShardInfoIntoContext(ctx, req.ShardInfo)

	// Find parquet shards for the time range
	shards, err := p.findParquetBlocks(ctx, blockMatchers)
	if err != nil {
		return fmt.Errorf("failed to find parquet shards: %w", err)
	}

	seriesSet := make([]prom_storage.ChunkSeriesSet, len(shards))
	errGroup, ctx := errgroup.WithContext(srv.Context())
	errGroup.SetLimit(p.concurrency)

	for i, shard := range shards {
		resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
			Id: shard.name,
		})
		errGroup.Go(func() error {
			ss, err := shard.Query(ctx, req.MinTime, req.MaxTime, req.SkipChunks, matchers)
			seriesSet[i] = ss
			return err
		})
	}

	if err = errGroup.Wait(); err != nil {
		return err
	}

	ss := convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger())
	for ss.Next() {
		cs := ss.At()
		cIter := cs.Iterator(nil)
		chunks := make([]storepb.AggrChunk, 0)
		for cIter.Next() {
			chunk := cIter.At()
			chunks = append(chunks, storepb.AggrChunk{
				MinTime: chunk.MinTime,
				MaxTime: chunk.MaxTime,
				Raw: &storepb.Chunk{
					Type: chunkToStoreEncoding(chunk.Chunk.Encoding()),
					Data: chunk.Chunk.Bytes(),
				},
			})
		}

		if err = srv.Send(storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.ZLabelsFromPromLabels(cs.Labels()),
			Chunks: chunks,
		})); err != nil {
			err = status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
			return
		}
	}

	if anyHints, err = types.MarshalAny(resHints); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
		return
	}

	if err = srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
		return
	}

	return nil
}

// LabelNames implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, ctx := spanlogger.New(ctx, "ParquetBucketStore.LabelNames")
	defer spanLog.Finish()

	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resHints := &hintspb.LabelNamesResponseHints{}

	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelNamesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label names request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	// Find parquet shards for the time range
	shards, err := p.findParquetBlocks(ctx, blockMatchers)
	if err != nil {
		return nil, fmt.Errorf("failed to find parquet shards: %w", err)
	}

	resNameSets := make([][]string, len(shards))
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.concurrency)

	for i, s := range shards {
		resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
			Id: s.name,
		})
		errGroup.Go(func() error {
			r, err := s.LabelNames(ctx, req.Limit, matchers)
			resNameSets[i] = r
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}
	result := util.MergeUnsortedSlices(int(req.Limit), resNameSets...)

	return &storepb.LabelNamesResponse{
		Names: result,
		Hints: anyHints,
	}, nil
}

// LabelValues implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	spanLog, ctx := spanlogger.New(ctx, "ParquetBucketStore.LabelValues")
	defer spanLog.Finish()

	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resHints := &hintspb.LabelValuesResponseHints{}
	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelValuesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label values request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	// Find parquet shards for the time range
	shards, err := p.findParquetBlocks(ctx, blockMatchers)
	if err != nil {
		return nil, fmt.Errorf("failed to find parquet shards: %w", err)
	}

	resNameValues := make([][]string, len(shards))
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.concurrency)

	for i, s := range shards {
		resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
			Id: s.name,
		})
		errGroup.Go(func() error {
			r, err := s.LabelValues(ctx, req.Label, req.Limit, matchers)
			resNameValues[i] = r
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}
	result := util.MergeUnsortedSlices(int(req.Limit), resNameValues...)

	return &storepb.LabelValuesResponse{
		Values: result,
		Hints:  anyHints,
	}, nil
}
