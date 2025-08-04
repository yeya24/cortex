package tsdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create test cache
func createTestCache(t *testing.T, config DiskBucketCacheConfig) (*DiskCache, func()) {
	cache, err := NewDiskCache("test-cache", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	diskCache := cache.(*DiskCache)
	cleanup := func() {
		diskCache.Close()
		if config.CacheDir != "" {
			os.RemoveAll(config.CacheDir)
		}
	}

	return diskCache, cleanup
}

// Helper function to create benchmark cache
func createBenchCache(b *testing.B, config DiskBucketCacheConfig) (*DiskCache, func()) {
	cache, err := NewDiskCache("bench-cache", config, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(b, err)

	diskCache := cache.(*DiskCache)
	cleanup := func() {
		diskCache.Close()
		if config.CacheDir != "" {
			os.RemoveAll(config.CacheDir)
		}
	}

	return diskCache, cleanup
}

func TestDiskCacheBasicOperations(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "disk_cache_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create disk cache configuration
	config := DiskBucketCacheConfig{
		CacheDir:         tempDir,
		MaxSizeBytes:     1024 * 1024 * 10, // 10MB
		TTLCheckInterval: time.Second,
	}

	// Create cache instance
	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Test storing and fetching data
	testData := map[string][]byte{
		"tenant:test:key1": []byte("value1"),
		"tenant:test:key2": []byte("value2"),
		"tenant:test:key3": []byte("value3"),
	}

	// Store data
	cache.Store(testData, time.Hour)

	// Fetch data
	result := cache.Fetch(context.Background(), []string{"tenant:test:key1", "tenant:test:key2", "tenant:test:key3"})

	// Verify results
	assert.Len(t, result, 3)
	assert.Equal(t, testData["tenant:test:key1"], result["tenant:test:key1"])
	assert.Equal(t, testData["tenant:test:key2"], result["tenant:test:key2"])
	assert.Equal(t, testData["tenant:test:key3"], result["tenant:test:key3"])
}

func TestDiskCacheTenantIsolation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_tenant_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:          tempDir,
		MaxSizeBytes:      1024 * 1024 * 10, // 10MB
		TTLCheckInterval:  time.Second,
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Store data for different tenants
	tenantAData := map[string][]byte{
		"tenant:userA:key1": []byte("valueA1"),
		"tenant:userA:key2": []byte("valueA2"),
	}
	tenantBData := map[string][]byte{
		"tenant:userB:key1": []byte("valueB1"),
		"tenant:userB:key2": []byte("valueB2"),
	}

	cache.Store(tenantAData, time.Hour)
	cache.Store(tenantBData, time.Hour)

	// Verify tenant directories exist
	tenantADir := filepath.Join(tempDir, "userA")
	tenantBDir := filepath.Join(tempDir, "userB")

	assert.DirExists(t, tenantADir)
	assert.DirExists(t, tenantBDir)

	// Verify data isolation
	resultA := cache.Fetch(context.Background(), []string{"tenant:userA:key1", "tenant:userA:key2"})
	resultB := cache.Fetch(context.Background(), []string{"tenant:userB:key1", "tenant:userB:key2"})

	assert.Len(t, resultA, 2)
	assert.Len(t, resultB, 2)
	assert.Equal(t, tenantAData["tenant:userA:key1"], resultA["tenant:userA:key1"])
	assert.Equal(t, tenantBData["tenant:userB:key1"], resultB["tenant:userB:key1"])
}

func TestDiskCacheTTL(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_ttl_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:          tempDir,
		MaxSizeBytes:      1024 * 1024 * 10, // 10MB
		TTLCheckInterval:  time.Second,
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Store data with different TTLs
	shortLivedData := map[string][]byte{
		"tenant:test:short-ttl": []byte("short-lived-data"),
	}
	longLivedData := map[string][]byte{
		"tenant:test:no-ttl": []byte("permanent-data"),
	}

	cache.Store(shortLivedData, 100*time.Millisecond) // Short TTL
	cache.Store(longLivedData, 0)                     // No TTL

	// Wait for short TTL to expire
	time.Sleep(300 * time.Millisecond)

	// Fetch data
	keys := []string{"tenant:test:short-ttl", "tenant:test:no-ttl"}
	result := cache.Fetch(context.Background(), keys)

	// Only the long-lived data should remain
	assert.Len(t, result, 1)
	assert.Contains(t, result, "tenant:test:no-ttl")
	assert.NotContains(t, result, "tenant:test:short-ttl")
}

func TestDiskCacheEvictionOnWrite(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_eviction_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Configure a small cache that will force eviction
	config := DiskBucketCacheConfig{
		CacheDir:          tempDir,
		MaxSizeBytes:      1024,        // Very small limit
		TTLCheckInterval:  time.Minute, // Long intervals to test immediate eviction
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Fill the cache with data
	data1 := map[string][]byte{
		"tenant:test:key1": make([]byte, 500), // 500 bytes
	}
	data2 := map[string][]byte{
		"tenant:test:key2": make([]byte, 400), // 400 bytes
	}

	// Store first set of data (total: 500 bytes)
	cache.Store(data1, time.Hour)

	// Store second set of data (total: 900 bytes, still under limit)
	cache.Store(data2, time.Hour)

	// Both should be available
	result := cache.Fetch(context.Background(), []string{"tenant:test:key1", "tenant:test:key2"})
	assert.Len(t, result, 2)

	// Now store data that will exceed the limit and force eviction
	data3 := map[string][]byte{
		"tenant:test:key3": make([]byte, 400), // This should trigger eviction
	}

	cache.Store(data3, time.Hour)

	// Verify eviction occurred - oldest entries should be evicted
	allKeys := []string{"tenant:test:key1", "tenant:test:key2", "tenant:test:key3"}
	result = cache.Fetch(context.Background(), allKeys)

	// Should have key3 and at most one other key due to size limits
	assert.Contains(t, result, "tenant:test:key3", "New entry should be present")
	assert.LessOrEqual(t, len(result), 2, "Should have evicted some entries")
}

func TestDiskCacheSizeLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_size_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:          tempDir,
		MaxSizeBytes:      1024, // 1KB limit
		TTLCheckInterval:  100 * time.Millisecond,
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Store data that exceeds the size limit
	largeData := make(map[string][]byte)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("tenant:test:large-key-%d", i)
		largeData[key] = make([]byte, 200) // 200 bytes each
	}

	cache.Store(largeData, time.Hour)

	// Wait for background size cleanup to run
	time.Sleep(250 * time.Millisecond)

	// Verify that not all entries are present (some should have been evicted)
	keys := make([]string, 0, len(largeData))
	for key := range largeData {
		keys = append(keys, key)
	}

	result := cache.Fetch(context.Background(), keys)
	assert.Less(t, len(result), len(largeData), "Some entries should have been evicted")
}

func TestDiskCacheFileIntegrity(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_integrity_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:          tempDir,
		MaxSizeBytes:      1024 * 1024,
		TTLCheckInterval:  time.Second,
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Store data
	testData := map[string][]byte{
		"tenant:test:integrity": []byte("test data for integrity check"),
	}
	cache.Store(testData, time.Hour)

	// Fetch data
	result := cache.Fetch(context.Background(), []string{"tenant:test:integrity"})
	assert.Len(t, result, 1)
	assert.Equal(t, testData["tenant:test:integrity"], result["tenant:test:integrity"])

	// Verify cache files exist
	tenantDir := filepath.Join(tempDir, "test")
	files, err := os.ReadDir(tenantDir)
	require.NoError(t, err)

	var cacheFiles, metaFiles int
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".cache") {
			cacheFiles++
		} else if strings.HasSuffix(file.Name(), ".meta") {
			metaFiles++
		}
	}

	assert.Equal(t, 1, cacheFiles, "Should have 1 cache file")
	assert.Equal(t, 1, metaFiles, "Should have 1 metadata file")
}

func TestDiskCacheConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		config DiskBucketCacheConfig
		errMsg string
	}{
		{
			name: "valid config",
			config: DiskBucketCacheConfig{
				CacheDir:     "/tmp/test",
				MaxSizeBytes: 1024,
			},
			errMsg: "",
		},
		{
			name: "empty cache dir",
			config: DiskBucketCacheConfig{
				MaxSizeBytes: 1024,
			},
			errMsg: "disk cache directory must be specified",
		},
		{
			name: "zero max size",
			config: DiskBucketCacheConfig{
				CacheDir:     "/tmp/test",
				MaxSizeBytes: 0,
			},
			errMsg: "disk cache max size must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.errMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			}
		})
	}
}

func TestDiskCacheTenantExtraction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_tenant_extraction_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024,
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	tests := []struct {
		key            string
		expectedTenant string
	}{
		{"tenant:user123:chunk:abc", "user123"},
		{"tenant:org456:metadata:def", "org456"},
		{"tenant:special-chars_123:key", "special-chars_123"},
		{"no-tenant-prefix", "default"},
		{"tenant:only-one-colon", "only-one-colon"},
		{"", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			actualTenant := cache.extractTenantID(tt.key)
			assert.Equal(t, tt.expectedTenant, actualTenant)
		})
	}
}

func TestDiskCacheErrorHandling(t *testing.T) {
	// Test with invalid directory permissions
	tempDir, err := os.MkdirTemp("", "disk_cache_error_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a subdirectory that we'll make read-only
	readOnlyDir := filepath.Join(tempDir, "readonly")
	err = os.MkdirAll(readOnlyDir, 0444) // Read-only permissions
	require.NoError(t, err)

	config := DiskBucketCacheConfig{
		CacheDir:     readOnlyDir,
		MaxSizeBytes: 1024 * 1024,
	}

	// This should still succeed because we can create the base directory
	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// But storing data may fail due to permission issues
	testData := map[string][]byte{
		"tenant:test:key": []byte("test value"),
	}

	// This might fail, but shouldn't crash
	cache.Store(testData, time.Hour)

	// Fetching should return empty result
	result := cache.Fetch(context.Background(), []string{"tenant:test:key"})
	assert.Empty(t, result)
}

func TestDiskCacheChecksumValidation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_checksum_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024,
	}

	cache, cleanup := createTestCache(t, config)
	defer cleanup()

	// Store some data
	testData := map[string][]byte{
		"tenant:test:checksum": []byte("data with checksum"),
	}
	cache.Store(testData, time.Hour)

	// Verify it can be fetched correctly
	result := cache.Fetch(context.Background(), []string{"tenant:test:checksum"})
	assert.Len(t, result, 1)
	assert.Equal(t, testData["tenant:test:checksum"], result["tenant:test:checksum"])
}

func TestDiskCacheMetrics(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disk_cache_metrics_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	reg := prometheus.NewRegistry()
	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024,
	}

	cache, err := NewDiskCache("metrics-test", config, log.NewNopLogger(), reg)
	require.NoError(t, err)
	defer cache.(*DiskCache).Close()

	// Store and fetch data to generate metrics
	testData := map[string][]byte{
		"tenant:test:key1": []byte("value1"),
		"tenant:test:key2": []byte("value2"),
	}

	cache.Store(testData, time.Hour)
	result := cache.Fetch(context.Background(), []string{"tenant:test:key1", "tenant:test:key2", "tenant:test:nonexistent"})

	assert.Len(t, result, 2) // Should find 2 out of 3 keys

	// Check that metrics were recorded (we don't validate exact values, just that they exist)
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	metricNames := make(map[string]bool)
	for _, mf := range metricFamilies {
		metricNames[*mf.Name] = true
	}

	expectedMetrics := []string{
		"thanos_cache_disk_requests_total",
		"thanos_cache_disk_hits_total",
		"thanos_cache_disk_misses_total",
		"thanos_cache_disk_size_bytes",
		"thanos_cache_disk_entries_total",
	}

	for _, expectedMetric := range expectedMetrics {
		assert.True(t, metricNames[expectedMetric], "Metric %s should be present", expectedMetric)
	}
}

// Benchmark tests
func BenchmarkDiskCacheStore(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "disk_cache_bench")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024 * 100, // 100MB
	}

	cache, cleanup := createBenchCache(b, config)
	defer cleanup()

	data := make([]byte, 1024) // 1KB data
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("tenant:test:bench-key-%d", i)
		cache.Store(map[string][]byte{key: data}, time.Hour)
	}
}

func BenchmarkDiskCacheFetch(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "disk_cache_bench")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	config := DiskBucketCacheConfig{
		CacheDir:     tempDir,
		MaxSizeBytes: 1024 * 1024 * 100, // 100MB
	}

	cache, cleanup := createBenchCache(b, config)
	defer cleanup()

	// Pre-populate cache
	data := make([]byte, 1024) // 1KB data
	keys := make([]string, 1000)
	testData := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("tenant:test:bench-key-%d", i)
		keys[i] = key
		testData[key] = data
	}
	cache.Store(testData, time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		cache.Fetch(context.Background(), []string{key})
	}
}
