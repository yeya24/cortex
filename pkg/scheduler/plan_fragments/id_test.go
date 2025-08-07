package plan_fragments

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

// This test makes sure that the metadata of the fragment can be successfully passed from the scheduler
// to the querier and to be correctly extracted

func TestFragmentMetadata(t *testing.T) {
	t.Run("basic injection and extraction", func(t *testing.T) {

		ctx := context.Background()
		fragment := Fragment{
			FragmentID: 123,
			IsRoot:     true,
		}
		queryID := uint64(456)

		ctx = InjectFragmentMetaData(ctx, fragment, queryID)

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
		fragment := Fragment{}
		ctx = InjectFragmentMetaData(ctx, fragment, 0)

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
