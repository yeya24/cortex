package distributed_execution

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryResultCache(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		cache := NewQueryResultCache()
		key := MakeFragmentKey(1, 1)

		// test InitWriting
		cache.InitWriting(key)
		result, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, StatusWriting, result.Status)
		require.False(t, cache.IsReady(key))

		// test SetComplete
		testData := []string{"test"}
		cache.SetComplete(key, testData)
		result, ok = cache.Get(key)
		require.True(t, ok)
		require.Equal(t, StatusDone, result.Status)
		require.Equal(t, testData, result.Data)
		require.True(t, cache.IsReady(key))

		// test SetError
		cache.SetError(key)
		result, ok = cache.Get(key)
		require.True(t, ok)
		require.Equal(t, StatusError, result.Status)
		require.False(t, cache.IsReady(key))
	})

	t.Run("expiration", func(t *testing.T) {
		cache := NewQueryResultCache()
		key := MakeFragmentKey(1, 1)

		cache.InitWriting(key)
		time.Sleep(DefaultTTL) // the default expiration time

		cache.CleanExpired()
		_, ok := cache.Get(key)
		require.False(t, ok, "Entry should have been cleaned up")
	})

	t.Run("concurrent operations", func(t *testing.T) {
		cache := NewQueryResultCache()
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines * 4)

		// write
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := MakeFragmentKey(uint64(routine), uint64(j))
					cache.SetComplete(key, j)
				}
			}(i)
		}

		// read
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := MakeFragmentKey(uint64(routine), uint64(j))
					cache.Get(key)
				}
			}(i)
		}

		// error
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := MakeFragmentKey(uint64(routine), uint64(j))
					cache.SetError(key)
				}
			}(i)
		}

		// status checkers
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := MakeFragmentKey(uint64(routine), uint64(j))
					cache.GetFragmentStatus(key)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("query clearing", func(t *testing.T) {
		cache := NewQueryResultCache()

		// add multiple fragments for different queries
		cache.SetComplete(MakeFragmentKey(1, 1), "data1")
		cache.SetComplete(MakeFragmentKey(1, 2), "data2")
		cache.SetComplete(MakeFragmentKey(2, 1), "data3")

		// clear query 1
		cache.ClearQuery(1)

		// check query 1 fragments are gone
		_, ok := cache.Get(MakeFragmentKey(1, 1))
		require.False(t, ok)
		_, ok = cache.Get(MakeFragmentKey(1, 2))
		require.False(t, ok)

		// check query 2 fragment still exists
		result, ok := cache.Get(MakeFragmentKey(2, 1))
		require.True(t, ok)
		require.Equal(t, "data3", result.Data)
	})

	t.Run("size tracking", func(t *testing.T) {
		cache := NewQueryResultCache()

		require.Equal(t, 0, cache.Size())

		// add entries
		cache.SetComplete(MakeFragmentKey(1, 1), "data1")
		cache.SetComplete(MakeFragmentKey(1, 2), "data2")
		require.Equal(t, 2, cache.Size())

		// clear entries
		cache.ClearQuery(1)
		require.Equal(t, 0, cache.Size())
	})
}
