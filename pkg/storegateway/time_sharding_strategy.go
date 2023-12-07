package storegateway

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type TimeShuffleShardingStrategy struct {
	shuffleSharding *ShuffleShardingStrategy
	logger          log.Logger
	duration        time.Duration
}

// NewTimeShuffleShardingStrategy makes a new ShuffleShardingStrategy.
func NewTimeShuffleShardingStrategy(shuffleSharding *ShuffleShardingStrategy, logger log.Logger, duration time.Duration) *TimeShuffleShardingStrategy {
	return &TimeShuffleShardingStrategy{
		shuffleSharding: shuffleSharding,
		logger:          logger,
		duration:        duration,
	}
}

// FilterUsers implements ShardingStrategy.
func (s *TimeShuffleShardingStrategy) FilterUsers(ctx context.Context, userIDs []string) []string {
	return s.shuffleSharding.FilterUsers(ctx, userIDs)
}

// FilterBlocks implements ShardingStrategy.
func (s *TimeShuffleShardingStrategy) FilterBlocks(ctx context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error {
	now := time.Now()
	newMetas := make(map[ulid.ULID]*metadata.Meta)
	for id, meta := range metas {
		// Keep all metas that are within duration.
		if meta.MinTime > now.Add(-s.duration).UnixMilli() {
			continue
		}
		delete(metas, id)
		newMetas[id] = meta
	}

	s.shuffleSharding.FilterBlocks(ctx, userID, newMetas, loaded, synced)
	for id, meta := range newMetas {
		metas[id] = meta
	}
	return nil
}
