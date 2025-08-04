package tsdb

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/cache"
)

const (
	// File extensions
	cacheFileExt    = ".cache"
	metadataFileExt = ".meta"
	tempFileExt     = ".tmp"
)

// CacheMetadata represents metadata stored alongside cache entries
type CacheMetadata struct {
	Key        string    `json:"key"`
	Size       uint64    `json:"size"`
	CreatedAt  time.Time `json:"created_at"`
	LastAccess time.Time `json:"last_access"`
	TTL        int64     `json:"ttl"` // TTL in milliseconds, 0 means no expiration
	Checksum   string    `json:"checksum"`
}

// DiskCache implements a file-based cache with tenant isolation
type DiskCache struct {
	name   string
	config DiskBucketCacheConfig
	logger log.Logger

	// Metrics
	requestTotal     prometheus.Counter
	hitsTotal        prometheus.Counter
	missesTotal      prometheus.Counter
	evictionsTotal   prometheus.Counter
	sizeBytes        prometheus.Gauge
	entriesTotal     prometheus.Gauge
	operationLatency *prometheus.HistogramVec
	evictionReasons  *prometheus.CounterVec

	mu               sync.RWMutex
	tenantDirs       map[string]string // Maps tenant ID to directory path
	currentSizeBytes uint64            // Cached total size in bytes
	currentEntries   uint64            // Cached total number of entries
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
}

// NewDiskCache creates a new disk-based cache
func NewDiskCache(name string, config DiskBucketCacheConfig, logger log.Logger, reg prometheus.Registerer) (cache.Cache, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Create base cache directory
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create cache directory")
	}

	ctx, cancel := context.WithCancel(context.Background())

	dc := &DiskCache{
		name:       name,
		config:     config,
		logger:     logger,
		tenantDirs: make(map[string]string),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Initialize metrics
	dc.requestTotal = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_cache_disk_requests_total",
		Help: "Total number of requests to the disk cache.",
		ConstLabels: map[string]string{
			"name": name,
		},
	})

	dc.hitsTotal = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_cache_disk_hits_total",
		Help: "Total number of cache hits.",
		ConstLabels: map[string]string{
			"name": name,
		},
	})

	dc.missesTotal = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_cache_disk_misses_total",
		Help: "Total number of cache misses.",
		ConstLabels: map[string]string{
			"name": name,
		},
	})

	dc.evictionsTotal = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_cache_disk_evictions_total",
		Help: "Total number of cache evictions.",
		ConstLabels: map[string]string{
			"name": name,
		},
	})

	dc.sizeBytes = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_cache_disk_size_bytes",
		Help: "Current size of the disk cache in bytes.",
		ConstLabels: map[string]string{
			"name": name,
		},
	})

	dc.entriesTotal = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_cache_disk_entries_total",
		Help: "Current number of entries in the disk cache.",
		ConstLabels: map[string]string{
			"name": name,
		},
	})

	dc.operationLatency = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_cache_disk_operation_duration_seconds",
		Help: "Latency of disk cache operations.",
		ConstLabels: map[string]string{
			"name": name,
		},
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	dc.evictionReasons = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_cache_disk_eviction_reasons_total",
		Help: "Total number of evictions by reason.",
		ConstLabels: map[string]string{
			"name": name,
		},
	}, []string{"reason"})

	// Initialize cached size and entry count by scanning existing files
	if err := dc.initializeCachedStats(); err != nil {
		level.Warn(dc.logger).Log("msg", "failed to initialize cached stats, starting with empty cache", "err", err)
		dc.currentSizeBytes = 0
		dc.currentEntries = 0
	}

	// Update metrics with initial values
	dc.sizeBytes.Set(float64(dc.currentSizeBytes))
	dc.entriesTotal.Set(float64(dc.currentEntries))

	// Start background cleanup workers
	dc.wg.Add(1)
	go dc.ttlCleanupWorker()

	level.Info(dc.logger).Log("msg", "disk cache initialized", "dir", config.CacheDir, "max_size_bytes", config.MaxSizeBytes, "initial_size_bytes", dc.currentSizeBytes, "initial_entries", dc.currentEntries)

	return dc, nil
}

// Name returns the cache name
func (dc *DiskCache) Name() string {
	return dc.name
}

// Store stores multiple key-value pairs
func (dc *DiskCache) Store(data map[string][]byte, ttl time.Duration) {
	start := time.Now()
	defer func() {
		dc.operationLatency.WithLabelValues("store").Observe(time.Since(start).Seconds())
	}()

	for key, value := range data {
		if err := dc.storeItem(key, value, ttl); err != nil {
			level.Error(dc.logger).Log("msg", "failed to store cache item", "key", key, "err", err)
		}
	}
}

// Fetch retrieves values for the given keys
func (dc *DiskCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	start := time.Now()
	defer func() {
		dc.operationLatency.WithLabelValues("fetch").Observe(time.Since(start).Seconds())
		dc.requestTotal.Add(float64(len(keys)))
	}()

	result := make(map[string][]byte)
	hits := 0

	for _, key := range keys {
		if value, found := dc.fetchItem(key); found {
			result[key] = value
			hits++
		}
	}

	dc.hitsTotal.Add(float64(hits))
	dc.missesTotal.Add(float64(len(keys) - hits))

	return result
}

// Close shuts down the cache and cleanup workers
func (dc *DiskCache) Close() {
	dc.cancel()
	dc.wg.Wait()
	level.Info(dc.logger).Log("msg", "disk cache closed")
}

// storeItem stores a single key-value pair
func (dc *DiskCache) storeItem(key string, value []byte, ttl time.Duration) error {
	tenantID := dc.extractTenantID(key)
	tenantDir, err := dc.getOrCreateTenantDir(tenantID)
	if err != nil {
		return errors.Wrap(err, "failed to get tenant directory")
	}

	valueSize := uint64(len(value))

	// Check if we need to evict entries before storing
	if err := dc.evictIfNecessary(valueSize); err != nil {
		return errors.Wrap(err, "failed to evict entries for new item")
	}

	fileID := dc.generateFileID(key)
	cacheFilePath := filepath.Join(tenantDir, fileID+cacheFileExt)
	metaFilePath := filepath.Join(tenantDir, fileID+metadataFileExt)

	// Calculate checksum
	checksum := dc.calculateChecksum(value)

	// Create metadata
	metadata := CacheMetadata{
		Key:        key,
		Size:       valueSize,
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		TTL:        int64(ttl / time.Millisecond),
		Checksum:   checksum,
	}

	// Write metadata
	metaData, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")
	}

	if err := dc.writeFile(metaFilePath, metaData); err != nil {
		return errors.Wrap(err, "failed to write metadata file")
	}

	// Write cache data
	if err := dc.writeFile(cacheFilePath, value); err != nil {
		// Clean up metadata file if data write fails
		os.Remove(metaFilePath)
		return errors.Wrap(err, "failed to write cache file")
	}

	// Update cached counters
	dc.mu.Lock()
	dc.currentSizeBytes += valueSize
	dc.currentEntries++
	dc.mu.Unlock()

	// Update metrics
	dc.sizeBytes.Add(float64(valueSize))
	dc.entriesTotal.Inc()

	return nil
}

// fetchItem retrieves a single value by key
func (dc *DiskCache) fetchItem(key string) ([]byte, bool) {
	tenantID := dc.extractTenantID(key)
	tenantDir, err := dc.getOrCreateTenantDir(tenantID)
	if err != nil {
		level.Error(dc.logger).Log("msg", "failed to get tenant directory", "key", key, "err", err)
		return nil, false
	}

	fileID := dc.generateFileID(key)
	cacheFilePath := filepath.Join(tenantDir, fileID+cacheFileExt)
	metaFilePath := filepath.Join(tenantDir, fileID+metadataFileExt)

	// Read metadata
	metaData, err := os.ReadFile(metaFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			level.Error(dc.logger).Log("msg", "failed to read metadata file", "key", key, "err", err)
		}
		return nil, false
	}

	var metadata CacheMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		level.Error(dc.logger).Log("msg", "failed to unmarshal metadata", "key", key, "err", err)
		return nil, false
	}

	// Check TTL
	if metadata.TTL > 0 {
		expireTime := metadata.CreatedAt.Add(time.Duration(metadata.TTL) * time.Millisecond)
		if time.Now().After(expireTime) {
			// Remove expired entry
			os.Remove(cacheFilePath)
			os.Remove(metaFilePath)

			// Update cached counters
			dc.mu.Lock()
			dc.currentSizeBytes -= metadata.Size
			dc.currentEntries--
			dc.mu.Unlock()

			// Update metrics
			dc.sizeBytes.Sub(float64(metadata.Size))
			dc.entriesTotal.Dec()
			return nil, false
		}
	}

	// Read cache data
	data, err := os.ReadFile(cacheFilePath)
	if err != nil {
		level.Error(dc.logger).Log("msg", "failed to read cache file", "key", key, "err", err)
		return nil, false
	}

	// Verify checksum
	if checksum := dc.calculateChecksum(data); checksum != metadata.Checksum {
		level.Error(dc.logger).Log("msg", "checksum mismatch for cache entry", "key", key, "expected", metadata.Checksum, "actual", checksum)
		// Remove corrupted entry
		os.Remove(cacheFilePath)
		os.Remove(metaFilePath)

		// Update cached counters
		dc.mu.Lock()
		dc.currentSizeBytes -= metadata.Size
		dc.currentEntries--
		dc.mu.Unlock()

		// Update metrics
		dc.sizeBytes.Sub(float64(metadata.Size))
		dc.entriesTotal.Dec()
		return nil, false
	}

	// Update last access time
	metadata.LastAccess = time.Now()
	if metaData, err := json.Marshal(metadata); err == nil {
		dc.writeFile(metaFilePath, metaData)
	}

	return data, true
}

// initializeCachedStats scans existing files to initialize cached size and entry counts
func (dc *DiskCache) initializeCachedStats() error {
	var totalSize uint64
	var totalEntries uint64

	err := filepath.WalkDir(dc.config.CacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, metadataFileExt) {
			metaData, readErr := os.ReadFile(path)
			if readErr != nil {
				level.Warn(dc.logger).Log("msg", "failed to read metadata file during initialization", "path", path, "err", readErr)
				return nil // Continue walking
			}

			var metadata CacheMetadata
			if unmarshalErr := json.Unmarshal(metaData, &metadata); unmarshalErr != nil {
				level.Warn(dc.logger).Log("msg", "failed to unmarshal metadata during initialization", "path", path, "err", unmarshalErr)
				return nil // Continue walking
			}

			// Check if entry has expired and should be cleaned up
			if metadata.TTL > 0 {
				expireTime := metadata.CreatedAt.Add(time.Duration(metadata.TTL) * time.Millisecond)
				if time.Now().After(expireTime) {
					// Remove expired entry immediately
					cacheFile := strings.TrimSuffix(path, metadataFileExt) + cacheFileExt
					os.Remove(cacheFile)
					os.Remove(path)
					return nil // Don't count this entry
				}
			}

			totalSize += metadata.Size
			totalEntries++
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "failed to walk cache directory during initialization")
	}

	dc.mu.Lock()
	dc.currentSizeBytes = totalSize
	dc.currentEntries = totalEntries
	dc.mu.Unlock()

	level.Info(dc.logger).Log("msg", "initialized cached stats", "size_bytes", totalSize, "entries", totalEntries)
	return nil
}

// evictIfNecessary checks if we need to evict entries before storing a new item
func (dc *DiskCache) evictIfNecessary(incomingSize uint64) error {
	dc.mu.RLock()
	currentSize := dc.currentSizeBytes
	dc.mu.RUnlock()

	// Check if adding the new item would exceed the size limit
	if currentSize+incomingSize <= dc.config.MaxSizeBytes {
		return nil // No eviction needed
	}

	// Calculate how much space we need to free
	spaceToFree := (currentSize + incomingSize) - dc.config.MaxSizeBytes

	level.Info(dc.logger).Log(
		"msg", "evicting entries for incoming write",
		"current_size", currentSize,
		"incoming_size", incomingSize,
		"max_size", dc.config.MaxSizeBytes,
		"space_to_free", spaceToFree,
	)

	return dc.evictLRUEntries(spaceToFree)
}

// getCachedSize returns the current cached total size of the cache
func (dc *DiskCache) getCachedSize() uint64 {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return dc.currentSizeBytes
}

// evictLRUEntries evicts the least recently used entries to free the specified amount of space
func (dc *DiskCache) evictLRUEntries(spaceToFree uint64) error {
	// Collect all entries with their last access times
	type entryInfo struct {
		metaPath   string
		cacheFile  string
		lastAccess time.Time
		size       uint64
	}

	var entries []entryInfo

	err := filepath.WalkDir(dc.config.CacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, metadataFileExt) {
			metaData, readErr := os.ReadFile(path)
			if readErr != nil {
				level.Error(dc.logger).Log("msg", "failed to read metadata file during eviction", "path", path, "err", readErr)
				return nil // Continue walking
			}

			var metadata CacheMetadata
			if unmarshalErr := json.Unmarshal(metaData, &metadata); unmarshalErr != nil {
				level.Error(dc.logger).Log("msg", "failed to unmarshal metadata during eviction", "path", path, "err", unmarshalErr)
				return nil // Continue walking
			}

			cacheFile := strings.TrimSuffix(path, metadataFileExt) + cacheFileExt
			entries = append(entries, entryInfo{
				metaPath:   path,
				cacheFile:  cacheFile,
				lastAccess: metadata.LastAccess,
				size:       metadata.Size,
			})
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "failed to walk cache directory during eviction")
	}

	// Sort by last access time (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastAccess.Before(entries[j].lastAccess)
	})

	// Evict entries until we've freed enough space
	var freedSpace uint64
	for _, entry := range entries {
		if freedSpace >= spaceToFree {
			break
		}

		// Remove cache and metadata files
		if err := os.Remove(entry.cacheFile); err != nil && !os.IsNotExist(err) {
			level.Error(dc.logger).Log("msg", "failed to remove cache file during eviction", "path", entry.cacheFile, "err", err)
		}

		if err := os.Remove(entry.metaPath); err != nil && !os.IsNotExist(err) {
			level.Error(dc.logger).Log("msg", "failed to remove metadata file during eviction", "path", entry.metaPath, "err", err)
		}

		freedSpace += entry.size

		// Update cached counters
		dc.mu.Lock()
		dc.currentSizeBytes -= entry.size
		dc.currentEntries--
		dc.mu.Unlock()

		// Update metrics
		dc.sizeBytes.Sub(float64(entry.size))
		dc.entriesTotal.Dec()
		dc.evictionsTotal.Inc()
		dc.evictionReasons.WithLabelValues("size_limit").Inc()
	}

	level.Info(dc.logger).Log(
		"msg", "completed LRU eviction",
		"space_freed", freedSpace,
		"space_requested", spaceToFree,
		"entries_evicted", len(entries),
	)

	return nil
}

// extractTenantID extracts tenant ID from keys like "tenant:user123:key"
func (dc *DiskCache) extractTenantID(key string) string {
	if strings.HasPrefix(key, "tenant:") {
		parts := strings.SplitN(key, ":", 3)
		if len(parts) >= 2 {
			return parts[1]
		}
	}
	return "default"
}

// getOrCreateTenantDir ensures the tenant directory exists and returns its path
func (dc *DiskCache) getOrCreateTenantDir(tenantID string) (string, error) {
	dc.mu.RLock()
	if dir, exists := dc.tenantDirs[tenantID]; exists {
		dc.mu.RUnlock()
		return dir, nil
	}
	dc.mu.RUnlock()

	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Double-check after acquiring write lock
	if dir, exists := dc.tenantDirs[tenantID]; exists {
		return dir, nil
	}

	// Create tenant directory
	tenantDir := filepath.Join(dc.config.CacheDir, tenantID)
	if err := os.MkdirAll(tenantDir, 0755); err != nil {
		return "", errors.Wrap(err, "failed to create tenant directory")
	}

	dc.tenantDirs[tenantID] = tenantDir
	return tenantDir, nil
}

// writeFile atomically writes data to a file
func (dc *DiskCache) writeFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tempFile := path + tempFileExt

	// Write to temporary file first
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return errors.Wrap(err, "failed to write temporary file")
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		os.Remove(tempFile)
		return errors.Wrap(err, "failed to create directory")
	}

	// Atomic rename
	if err := os.Rename(tempFile, path); err != nil {
		os.Remove(tempFile)
		return errors.Wrap(err, "failed to rename temporary file")
	}

	// Sync if configured
	if dc.config.SyncWrites {
		if file, err := os.OpenFile(path, os.O_RDONLY, 0); err == nil {
			file.Sync()
			file.Close()
		}
	}

	return nil
}

// generateFileID generates a consistent filename for a cache key
func (dc *DiskCache) generateFileID(key string) string {
	hash := md5.Sum([]byte(key))
	return hex.EncodeToString(hash[:])
}

// calculateChecksum calculates MD5 checksum of data
func (dc *DiskCache) calculateChecksum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// ttlCleanupWorker periodically removes expired entries
func (dc *DiskCache) ttlCleanupWorker() {
	defer dc.wg.Done()

	ticker := time.NewTicker(dc.config.TTLCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dc.ctx.Done():
			return
		case <-ticker.C:
			dc.cleanupExpiredEntries()
		}
	}
}

// cleanupExpiredEntries removes entries that have exceeded their TTL
func (dc *DiskCache) cleanupExpiredEntries() {
	var expiredCount int
	var freedBytes uint64

	err := filepath.WalkDir(dc.config.CacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, metadataFileExt) {
			metaData, readErr := os.ReadFile(path)
			if readErr != nil {
				level.Error(dc.logger).Log("msg", "failed to read metadata file during TTL cleanup", "path", path, "err", readErr)
				return nil // Continue walking
			}

			var metadata CacheMetadata
			if unmarshalErr := json.Unmarshal(metaData, &metadata); unmarshalErr != nil {
				level.Error(dc.logger).Log("msg", "failed to unmarshal metadata during TTL cleanup", "path", path, "err", unmarshalErr)
				return nil // Continue walking
			}

			// Check if entry has expired
			if metadata.TTL > 0 {
				expireTime := metadata.CreatedAt.Add(time.Duration(metadata.TTL) * time.Millisecond)
				if time.Now().After(expireTime) {
					// Remove expired entry
					cacheFile := strings.TrimSuffix(path, metadataFileExt) + cacheFileExt
					os.Remove(cacheFile)
					os.Remove(path)

					expiredCount++
					freedBytes += metadata.Size

					// Update cached counters
					dc.mu.Lock()
					dc.currentSizeBytes -= metadata.Size
					dc.currentEntries--
					dc.mu.Unlock()

					// Update metrics
					dc.sizeBytes.Sub(float64(metadata.Size))
					dc.entriesTotal.Dec()
					dc.evictionsTotal.Inc()
					dc.evictionReasons.WithLabelValues("ttl_expired").Inc()
				}
			}
		}

		return nil
	})

	if err != nil {
		level.Error(dc.logger).Log("msg", "error during TTL cleanup", "err", err)
	}

	if expiredCount > 0 {
		level.Info(dc.logger).Log("msg", "TTL cleanup completed", "expired_entries", expiredCount, "freed_bytes", freedBytes)
	}
}
