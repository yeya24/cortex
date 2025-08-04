# Local Disk Cache Usage Guide

## Overview

The local disk cache provides a high-performance, tenant-isolated caching layer that can be used standalone or as part of a multilevel cache hierarchy in Cortex. It features automatic size management, TTL-based expiration, LRU eviction, and tenant isolation.

## Quick Start

### 1. Basic Configuration

```yaml
# Single disk cache
chunks_cache:
  backend: "disk"
  disk:
    cache_dir: "/var/cache/cortex"
    max_size_bytes: 107374182400  # 100GB
```

### 2. Multilevel Configuration

```yaml
# Memory + Disk hierarchy 
chunks_cache:
  backend: "inmemory,disk"
  inmemory:
    max_size_bytes: 2147483648    # 2GB fast cache
  disk:
    cache_dir: "/cache/cortex"
    max_size_bytes: 107374182400  # 100GB persistent cache
```

## Configuration Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `cache_dir` | Directory for cache storage | Required | `/var/cache/cortex` |
| `max_size_bytes` | Maximum cache size | Required | `107374182400` (100GB) |
| `sync_writes` | Synchronous disk writes | `false` | `true` for durability |
| `ttl_check_interval` | TTL cleanup frequency | `5m` | `"10m"` |



## Tenant Isolation

### Key Format for Tenant Isolation

The cache automatically extracts tenant IDs from cache keys:

```go
// Tenant-specific keys
"tenant:user123:chunk:01234567890abcdef"
"tenant:org456:metadata:block:98765432"

// Default tenant keys  
"chunk:01234567890abcdef"  // Goes to "default" tenant
```

### Directory Structure

```
/var/cache/cortex/
├── user123/
│   ├── abc123def456.cache  # Raw cache data
│   └── abc123def456.meta   # JSON metadata
├── org456/
│   └── ...
└── default/
    └── ...
```

**Note:** Cache files are stored by MD5 hash of the key for consistent naming.

## Running Tests

### Unit Tests

```bash
# Run all disk cache tests
go test ./pkg/storage/tsdb -run TestDiskCache -v

# Run specific test categories
go test ./pkg/storage/tsdb -run TestDiskCacheBasic -v
go test ./pkg/storage/tsdb -run TestDiskCacheTenant -v
go test ./pkg/storage/tsdb -run TestDiskCacheEvictionOnWrite -v
```

### Integration Tests

```bash
# Run integration tests
go test ./pkg/storage/tsdb -run TestDiskCacheInterfaceCompliance -v
go test ./pkg/storage/tsdb -run TestDiskCacheRecoveryAfterRestart -v
```

### Benchmarks

```bash
# Run performance benchmarks
go test ./pkg/storage/tsdb -bench=BenchmarkDiskCache -v

# Compare store vs fetch performance
go test ./pkg/storage/tsdb -bench=BenchmarkDiskCacheStore -v
go test ./pkg/storage/tsdb -bench=BenchmarkDiskCacheFetch -v
```

### Stress Tests

```bash
# Run stress tests (takes longer)
go test ./pkg/storage/tsdb -run TestDiskCacheUnderStress -v
go test ./pkg/storage/tsdb -run TestDiskCacheWithConcurrentAccess -v
```

## Performance Tuning

### For Maximum Speed

```yaml
disk:
  sync_writes: false         # Async writes
  ttl_check_interval: "10m"  # Less frequent cleanup
```

### For Maximum Durability

```yaml
disk:
  sync_writes: true          # Sync every write
  ttl_check_interval: "1m"   # Frequent cleanup
```

### For Space Efficiency

```yaml
disk:
  max_size_bytes: 53687091200 # 50GB limit
  ttl_check_interval: "5m"    # Regular cleanup to free space
```

## Monitoring

### Key Metrics to Monitor

```
# Hit/Miss Ratios
thanos_cache_disk_hits_total
thanos_cache_disk_misses_total
thanos_cache_disk_requests_total

# Performance
thanos_cache_disk_operation_duration_seconds{operation="store"}
thanos_cache_disk_operation_duration_seconds{operation="fetch"}

# Resource Usage
thanos_cache_disk_size_bytes
thanos_cache_disk_entries_total

# Eviction Activity
thanos_cache_disk_evictions_total
thanos_cache_disk_eviction_reasons_total{reason="size_limit"}
thanos_cache_disk_eviction_reasons_total{reason="ttl_expired"}
```

### Grafana Dashboard Example

```json
{
  "targets": [
    {
      "expr": "rate(thanos_cache_disk_hits_total[5m])",
      "legendFormat": "Cache Hits/sec"
    },
    {
      "expr": "rate(thanos_cache_disk_misses_total[5m])",
      "legendFormat": "Cache Misses/sec"
    },
    {
      "expr": "thanos_cache_disk_size_bytes",
      "legendFormat": "Cache Size Bytes"
    },
    {
      "expr": "rate(thanos_cache_disk_evictions_total[5m])",
      "legendFormat": "Evictions/sec"
    }
  ]
}
```

### Key Performance Indicators (KPIs)

```
# Cache Hit Rate
(rate(thanos_cache_disk_hits_total[5m]) / rate(thanos_cache_disk_requests_total[5m])) * 100

# Cache Utilization
(thanos_cache_disk_size_bytes / max_size_bytes) * 100

# Eviction Rate
rate(thanos_cache_disk_evictions_total[5m])
```

## Troubleshooting

### Common Issues

1. **High disk usage**
   - Check `thanos_cache_disk_size_bytes`
   - Reduce `max_size_bytes` or `max_size_per_tenant`
   - Increase cleanup frequency

2. **Poor hit rates**
   - Monitor cache evictions with `thanos_cache_disk_evictions_total`
   - Increase cache size
   - Check TTL settings

3. **Slow performance**
   - Use faster storage (NVMe SSD vs SATA SSD vs HDD)
   - Increase `ttl_check_interval` for less frequent cleanup
   - Set `sync_writes: false` for async I/O

4. **High eviction rates**
   - Check `thanos_cache_disk_eviction_reasons_total` for size_limit vs ttl_expired
   - Increase `max_size_bytes` if size-limited
   - Adjust TTL if time-limited

### Debug Commands

```bash
# Check cache directory structure
find /var/cache/cortex -type f -name "*.cache" | head -10
find /var/cache/cortex -type f -name "*.meta" | head -5

# Monitor cache size by tenant
du -sh /var/cache/cortex/*

# Check for orphaned files (no .cache file without .meta)
find /var/cache/cortex -name "*.cache" | while read f; do
  meta="${f%.cache}.meta"
  [ ! -f "$meta" ] && echo "Orphaned cache file: $f"
done

# Check cache metadata
find /var/cache/cortex -name "*.meta" -exec head -5 {} \;
```

## Best Practices

### 1. **Storage Setup**
- Use NVMe SSD storage for best performance
- Mount with `noatime` for better I/O performance
- Ensure adequate disk space (2x max cache size for safety)
- Use local instance storage when available (faster than EBS)

### 2. **Configuration**
- Start with conservative size limits (50% of available disk)
- Use async writes unless durability is critical
- Set reasonable TTL check intervals (5-15 minutes)
- Enable per-tenant limits for multi-tenant environments

### 3. **Monitoring**
- Set up alerts for high eviction rates (>1000/min)
- Monitor hit/miss ratios (target >80% hit rate)
- Track disk space usage (alert at >80% utilization)
- Monitor cache size vs configured limits

### 4. **Maintenance**
- Regularly review cache effectiveness metrics
- Adjust size limits based on actual usage patterns
- Consider cache warming strategies for cold starts
- Plan for cache directory cleanup during maintenance windows

### 5. **Operational Considerations**
- Cache is ephemeral - can be safely deleted
- No backup required (cache will repopulate)
- Safe to restart with empty cache directory
- Consider cache locality when scaling horizontally

## Example Deployment

### Kubernetes StatefulSet with Persistent Volume

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cortex-querier
spec:
  serviceName: cortex-querier
  replicas: 3
  template:
    spec:
      containers:
      - name: cortex
        volumeMounts:
        - name: cache-volume
          mountPath: /cache
        env:
        - name: CORTEX_CHUNKS_CACHE_BACKEND
          value: "disk"
        - name: CORTEX_CHUNKS_CACHE_DISK_CACHE_DIR
          value: "/cache"
        - name: CORTEX_CHUNKS_CACHE_DISK_MAX_SIZE_BYTES
          value: "107374182400"
  volumeClaimTemplates:
  - metadata:
      name: cache-volume
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 120Gi  # 20% larger than cache size
```

### Kubernetes with EmptyDir (Ephemeral)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cortex-querier
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: cortex
        volumeMounts:
        - name: cache-volume
          mountPath: /cache
      volumes:
      - name: cache-volume
        emptyDir:
          sizeLimit: 120Gi
```

### Docker Compose

```yaml
version: '3.8'
services:
  cortex-querier:
    image: cortexproject/cortex:latest
    volumes:
      - cache-data:/cache
    environment:
      - CORTEX_CHUNKS_CACHE_BACKEND=disk
      - CORTEX_CHUNKS_CACHE_DISK_CACHE_DIR=/cache
      - CORTEX_CHUNKS_CACHE_DISK_MAX_SIZE_BYTES=107374182400

volumes:
  cache-data:
    driver: local
```

## AWS Deployment Considerations

### Instance Store vs EBS

- **Instance Store (Recommended)**: Faster, ephemeral, free
  - Use for cache workloads where data loss is acceptable
  - Significantly faster I/O than EBS
  
- **EBS**: Persistent, but slower and costs extra
  - Use with `deleteOnTermination: true` to keep stateless
  - Consider `gp3` volumes for better performance

### Example EBS Configuration

```yaml
# In your EC2 launch template or instance configuration
BlockDeviceMappings:
  - DeviceName: /dev/xvdf
    Ebs:
      VolumeSize: 120
      VolumeType: gp3
      Iops: 3000
      Throughput: 125
      DeleteOnTermination: true
```

This disk cache implementation provides a robust, scalable caching solution that maintains the stateless nature of your Cortex deployment while providing significant performance improvements through local SSD caching. 