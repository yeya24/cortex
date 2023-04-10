package storegateway

import (
	"context"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/oklog/ulid"
)

type LocalBlocksFinder struct {
	services.Service
	blockIDs []ulid.ULID
}

func (d *LocalBlocksFinder) GetBlocks(_ context.Context, _ string, _, _ int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	res := make([]*bucketindex.Block, len(d.blockIDs))
	for i, block := range d.blockIDs {
		res[i] = &bucketindex.Block{
			ID: block,
		}
	}
	return res, nil, nil
}
