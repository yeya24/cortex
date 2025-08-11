package distributed_execution

import (
	"context"
	"github.com/cortexproject/cortex/pkg/scheduler/plan_fragments"
	"github.com/stretchr/testify/require"
	"testing"
)

// This test makes sure that the metadata of the fragment can be successfully passed from the scheduler
// to the querier and to be correctly extracted

func TestFragmentMetadata(t *testing.T) {
	t.Run("basic injection and extraction", func(t *testing.T) {

		ctx := context.Background()
		fragment := plan_fragments.Fragment{
			FragmentID: 123,
			IsRoot:     true,
		}
		queryID := uint64(456)

		ctx = InjectFragmentMetaData(ctx, fragment.FragmentID, queryID, fragment.IsRoot, fragment.ChildIDs)

		isRoot, qID, fID, ok := ExtractFragmentMetaData(ctx)
		require.True(t, ok)
		require.True(t, isRoot)
		require.Equal(t, queryID, qID)
		require.Equal(t, fragment.FragmentID, fID)
	})

	t.Run("extraction from empty context", func(t *testing.T) {
		ctx := context.Background()
		isRoot, queryID, fragmentID, ok := ExtractFragmentMetaData(ctx)

		require.False(t, ok)
		require.False(t, isRoot)
		require.Equal(t, uint64(0), queryID)
		require.Equal(t, uint64(0), fragmentID)
	})

	t.Run("zero values", func(t *testing.T) {
		ctx := context.Background()
		fragment := plan_fragments.Fragment{}
		ctx = InjectFragmentMetaData(ctx, fragment.FragmentID, 0, fragment.IsRoot, fragment.ChildIDs)

		isRoot, queryID, fragmentID, ok := ExtractFragmentMetaData(ctx)
		require.True(t, ok)
		require.False(t, isRoot)
		require.Equal(t, uint64(0), queryID)
		require.Equal(t, uint64(0), fragmentID)
	})

	t.Run("type safety", func(t *testing.T) {
		ctx := context.Background()

		ctx = context.WithValue(ctx, fragmentMetadataKey{}, "wrong type")

		// should fail gracefully
		isRoot, queryID, fragmentID, ok := ExtractFragmentMetaData(ctx)
		require.False(t, ok)
		require.False(t, isRoot)
		require.Equal(t, uint64(0), queryID)
		require.Equal(t, uint64(0), fragmentID)
	})
}
