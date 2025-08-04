package tsdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/cache"
)

func TestDiskCacheInterfaceCompliance(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_interface_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024,
	}

	var cacheInterface cache.Cache
	cacheInterface, err = NewDiskCache("interface-test", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	diskCache := cacheInterface.(*DiskCache)
	defer diskCache.Close()

	// Test that it implements the cache.Cache interface
	assert.NotNil(t, cacheInterface)

	// Test basic interface methods
	data := map[string][]byte{
		"tenant:test:key": []byte("value"),
	}

	cacheInterface.Store(data, time.Hour)
	result := cacheInterface.Fetch(context.Background(), []string{"tenant:test:key"})
	assert.Equal(t, data["tenant:test:key"], result["tenant:test:key"])
}

func TestDiskCacheBackfillBehavior(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_backfill_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 10 * 1024 * 1024,
	}

	diskCache, err := NewDiskCache("disk", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	defer diskCache.(*DiskCache).Close()

	// Store data directly in disk cache
	diskData := map[string][]byte{
		"tenant:test:disk-only": []byte("disk-value"),
	}
	diskCache.Store(diskData, time.Hour)

	// Verify it can be retrieved
	result := diskCache.Fetch(context.Background(), []string{"tenant:test:disk-only"})
	assert.Len(t, result, 1)
	assert.Equal(t, diskData["tenant:test:disk-only"], result["tenant:test:disk-only"])
}

func TestDiskCacheUnderStress(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_stress_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:          tempDir,
		MaxSizeBytes:      1024 * 1024, // 1MB limit to trigger evictions
		TTLCheckInterval:  100 * time.Millisecond,
	}

	cache, err := NewDiskCache("stress-test", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	defer cache.(*DiskCache).Close()

	// Stress test with many concurrent operations
	const numGoroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent store operations
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("tenant:stress:g%d:key%d", goroutineID, j)
				data := map[string][]byte{
					key: []byte(fmt.Sprintf("value-g%d-k%d", goroutineID, j)),
				}
				cache.Store(data, time.Minute)
			}
		}(i)
	}

	wg.Wait()

	// Test concurrent fetch operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("tenant:stress:g%d:key%d", goroutineID, j)
				cache.Fetch(context.Background(), []string{key})
			}
		}(i)
	}

	wg.Wait()

	// Verify the cache is still functional
	testKey := "tenant:test:final"
	testData := map[string][]byte{
		testKey: []byte("final-test-value"),
	}
	cache.Store(testData, time.Hour)

	result := cache.Fetch(context.Background(), []string{testKey})
	assert.Equal(t, testData[testKey], result[testKey])
}

func TestDiskCacheWithConcurrentAccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_concurrent_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 10 * 1024 * 1024, // 10MB
	}

	cache, err := NewDiskCache("concurrent-test", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	defer cache.(*DiskCache).Close()

	// Test concurrent access with different keys per goroutine to avoid overwrites
	const numGoroutines = 5

	var wg sync.WaitGroup

	// Multiple goroutines storing different keys
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			key := fmt.Sprintf("tenant:test:key-%d", goroutineID)
			data := map[string][]byte{
				key: []byte(fmt.Sprintf("value-from-goroutine-%d", goroutineID)),
			}
			cache.Store(data, time.Hour)
		}(i)
	}

	wg.Wait()

	// Give a small buffer for writes to complete
	time.Sleep(50 * time.Millisecond)

	// Multiple goroutines reading their respective keys
	results := make([][]byte, numGoroutines)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			key := fmt.Sprintf("tenant:test:key-%d", goroutineID)
			result := cache.Fetch(context.Background(), []string{key})
			if value, exists := result[key]; exists {
				results[goroutineID] = value
			}
		}(i)
	}

	wg.Wait()

	// Verify we got results and there were no panics
	nonNilResults := 0
	for i, result := range results {
		if result != nil {
			nonNilResults++
			// Verify the content is correct
			expectedValue := fmt.Sprintf("value-from-goroutine-%d", i)
			assert.Equal(t, expectedValue, string(result), "Content should match for goroutine %d", i)
		}
	}
	assert.Greater(t, nonNilResults, 0, "At least some goroutines should have retrieved their values")

	// Also test that a single shared key works
	sharedKey := "tenant:test:shared-final"
	cache.Store(map[string][]byte{sharedKey: []byte("shared-value")}, time.Hour)

	// Multiple readers for shared key
	sharedResults := make([]bool, numGoroutines)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			result := cache.Fetch(context.Background(), []string{sharedKey})
			if value, exists := result[sharedKey]; exists && string(value) == "shared-value" {
				sharedResults[goroutineID] = true
			}
		}(i)
	}
	wg.Wait()

	// At least some should have read the shared value successfully
	sharedSuccesses := 0
	for _, success := range sharedResults {
		if success {
			sharedSuccesses++
		}
	}
	assert.Greater(t, sharedSuccesses, 0, "At least some goroutines should have read the shared value")
}

func TestDiskCacheRecoveryAfterRestart(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_recovery_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024,
	}

	// Create first cache instance and store data
	cache1, err := NewDiskCache("recovery-test", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	testData := map[string][]byte{
		"tenant:test:persistent": []byte("persistent-value"),
	}
	cache1.Store(testData, time.Hour)

	// Verify data exists
	result := cache1.Fetch(context.Background(), []string{"tenant:test:persistent"})
	assert.Equal(t, testData["tenant:test:persistent"], result["tenant:test:persistent"])

	// Close first cache
	cache1.(*DiskCache).Close()

	// Create second cache instance with same config (simulates restart)
	cache2, err := NewDiskCache("recovery-test", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	defer cache2.(*DiskCache).Close()

	// Data should still be accessible after "restart"
	result = cache2.Fetch(context.Background(), []string{"tenant:test:persistent"})
	assert.Equal(t, testData["tenant:test:persistent"], result["tenant:test:persistent"])
}
