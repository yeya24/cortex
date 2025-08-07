package plan_fragments

import (
	"context"
)

type fragmentMetadataKey struct{}

type fragmentMetadata struct {
	queryID    uint64
	fragmentID uint64
	isRoot     bool
}

func InjectFragmentMetaData(ctx context.Context, fragment Fragment, queryID uint64) context.Context {
	return context.WithValue(ctx, fragmentMetadataKey{}, fragmentMetadata{
		queryID:    queryID,
		fragmentID: fragment.FragmentID,
		isRoot:     fragment.IsRoot,
	})
}

func ExtractFragmentMetaData(ctx context.Context) (isRoot bool, queryID uint64, fragmentID uint64, ok bool) {
	metadata, ok := ctx.Value(fragmentMetadataKey{}).(fragmentMetadata)
	if !ok {
		return false, 0, 0, false
	}
	return metadata.isRoot, metadata.queryID, metadata.fragmentID, true
}
