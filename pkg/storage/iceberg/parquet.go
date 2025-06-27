package iceberg

import (
	"context"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/thanos-io/objstore"
)

type ParquetShard struct {
	dataFile iceberg.DataFile
	bkt      objstore.Bucket
	f        *storage.ParquetFile
}

func NewParquetShard(ctx context.Context, dataFile iceberg.DataFile, bkt objstore.Bucket) (*ParquetShard, error) {
	r := storage.NewBucketReadAt(ctx, dataFile.FilePath(), bkt)
	size := dataFile.FileSizeBytes()
	fileOptions := []parquet.FileOption{
		parquet.SkipMagicBytes(true),
		parquet.ReadBufferSize(100 * 1024),
		parquet.SkipBloomFilters(true),
	}
	shardOptions := []storage.ShardOption{
		storage.WithFileOptions(fileOptions...),
		storage.WithOptimisticReader(true),
	}
	f, err := storage.OpenFile(r, size, shardOptions...)
	if err != nil {
		return nil, err
	}
	return &ParquetShard{
		dataFile: dataFile,
		bkt:      bkt,
		f:        f,
	}, nil
}

type cacheInterface[T any] interface {
	Get(path string) T
	Set(path string, reader T)
}

type Cache[T any] struct {
	cache *lru.Cache[string, T]
	name  string
}

func newCache[T any](name string, size int) cacheInterface[T] {
	if size <= 0 {
		return &noopCache[T]{}
	}
	cache, _ := lru.NewWithEvict(size, func(key string, value T) {
	})

	return &Cache[T]{
		cache: cache,
		name:  name,
	}
}

func (c *Cache[T]) Get(path string) (r T) {
	if reader, ok := c.cache.Get(path); ok {
		return reader
	}
	return
}

func (c *Cache[T]) Set(path string, reader T) {
	c.cache.Add(path, reader)
}

type noopCache[T any] struct {
}

func (n noopCache[T]) Get(_ string) (r T) {
	return
}

func (n noopCache[T]) Set(_ string, _ T) {

}
