package distributed_execution

import (
	"context"
)

type fragmentMetadataKey struct{}

type fragmentMetadata struct {
	queryID       uint64
	fragmentID    uint64
	childIDToAddr map[uint64]string
	isRoot        bool
}

func InjectFragmentMetaData(ctx context.Context, fragmentID uint64, queryID uint64, isRoot bool, childIDs []uint64, childAddr []string) context.Context {
	childIDToAddr := make(map[uint64]string)
	for i, childID := range childIDs {
		childIDToAddr[childID] = childAddr[i]
	}

	return context.WithValue(ctx, fragmentMetadataKey{}, fragmentMetadata{
		queryID:       queryID,
		fragmentID:    fragmentID,
		childIDToAddr: childIDToAddr,
		isRoot:        isRoot,
	})
}

func ExtractFragmentMetaData(ctx context.Context) (isRoot bool, queryID uint64, fragmentID uint64, childAddrs map[uint64]string, ok bool) {
	metadata, ok := ctx.Value(fragmentMetadataKey{}).(fragmentMetadata)
	if !ok {
		return false, 0, 0, nil, false
	}
	return metadata.isRoot, metadata.queryID, metadata.fragmentID, metadata.childIDToAddr, true
}
