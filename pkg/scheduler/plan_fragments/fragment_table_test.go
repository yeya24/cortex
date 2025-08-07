package plan_fragments

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This test checks whether the hashtable for fragment-querier mapping gives the expected value
// It also checks if it remains functionable during a multi-thread/concurrent read & write situation

func TestSchedulerCoordination(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		table := NewFragmentTable()
		table.AddMapping(uint64(0), uint64(1), "localhost:8000")
		table.AddMapping(uint64(0), uint64(2), "localhost:8001")

		result, exist := table.GetMapping(uint64(0), []uint64{1, 2})
		require.True(t, exist)
		require.Equal(t, []string{"localhost:8000", "localhost:8001"}, result)

		result, exist = table.GetMapping(uint64(0), []uint64{1, 3})
		require.False(t, exist)
		require.Empty(t, result)

		result, exist = table.GetMapping(uint64(0), []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"localhost:8000"}, result)

		table.ClearMappings(uint64(0))
		result, exist = table.GetMapping(uint64(0), []uint64{1})
		require.False(t, exist)
		require.Empty(t, result)
	})

	t.Run("concurrent operations", func(t *testing.T) {
		table := NewFragmentTable()
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines * 3) // writers, readers, and clearers

		// writers
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					queryID := uint64(routine)
					fragmentID := uint64(j)
					addr := fmt.Sprintf("localhost:%d", j)
					table.AddMapping(queryID, fragmentID, addr)
				}
			}(i)
		}

		// readers
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					queryID := uint64(routine)
					fragmentIDs := []uint64{uint64(j)}
					table.GetMapping(queryID, fragmentIDs)
				}
			}(i)
		}

		// clearers
		for i := 0; i < numGoroutines; i++ {
			go func(routine int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					queryID := uint64(routine)
					table.ClearMappings(queryID)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("edge cases", func(t *testing.T) {
		table := NewFragmentTable()

		// test empty fragment IDs
		result, exist := table.GetMapping(0, []uint64{})
		require.True(t, exist)
		require.Empty(t, result)

		// test clearing non-existent query
		table.ClearMappings(999)
		require.NotPanics(t, func() {
			table.ClearMappings(999)
		})

		// test overwriting mapping
		table.AddMapping(1, 1, "addr1")
		table.AddMapping(1, 1, "addr2")
		result, exist = table.GetMapping(1, []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"addr2"}, result)

		// test multiple queries
		table.AddMapping(1, 1, "addr1")
		table.AddMapping(2, 1, "addr2")
		result, exist = table.GetMapping(1, []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"addr1"}, result)

		result, exist = table.GetMapping(2, []uint64{1})
		require.True(t, exist)
		require.Equal(t, []string{"addr2"}, result)
	})
}

func TestFragmentTableStress(t *testing.T) {
	table := NewFragmentTable()
	done := make(chan bool)
	const duration = 2 * time.Second

	// add counters to check the number of times each
	// components are actually executed
	var (
		addCount   atomic.Uint64
		readCount  atomic.Uint64
		clearCount atomic.Uint64
		errorCount atomic.Uint64
	)

	go func() {
		i := uint64(0)
		for {
			select {
			case <-done:
				return
			default:
				func() {
					defer func() {
						if r := recover(); r != nil {
							errorCount.Add(1)
						}
					}()
					table.AddMapping(i, i, fmt.Sprintf("addr%d", i))
					addCount.Add(1)
					i++
				}()
			}
		}
	}()

	go func() {
		i := uint64(0)
		for {
			select {
			case <-done:
				return
			default:
				func() {
					defer func() {
						if r := recover(); r != nil {
							errorCount.Add(1)
						}
					}()
					_, _ = table.GetMapping(i, []uint64{i})
					readCount.Add(1)
					i++
				}()
			}
		}
	}()

	go func() {
		i := uint64(0)
		for {
			select {
			case <-done:
				return
			default:
				func() {
					defer func() {
						if r := recover(); r != nil {
							errorCount.Add(1)
						}
					}()
					table.ClearMappings(i)
					clearCount.Add(1)
					i++
				}()
			}
		}
	}()

	// progress monitoring
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				t.Logf("Progress - Adds: %d, Reads: %d, Clears: %d, Errors: %d",
					addCount.Load(),
					readCount.Load(),
					clearCount.Load(),
					errorCount.Load(),
				)
			}
		}
	}()

	time.Sleep(duration)
	close(done)

	t.Logf("Final counts - Adds: %d, Reads: %d, Clears: %d, Errors: %d",
		addCount.Load(),
		readCount.Load(),
		clearCount.Load(),
		errorCount.Load(),
	)

	require.Equal(t, uint64(0), errorCount.Load(), "stress test produced errors")
}
