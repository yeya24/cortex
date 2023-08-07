package tsdb

import (
	"flag"
	"github.com/alecthomas/units"
	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/model"
)

type InMemoryCacheConfig struct {
	// MaxSize represents overall maximum number of bytes cache can contain.
	MaxSizeBytes uint64 `yaml:"max_size_bytes"`
	// MaxItemSize represents maximum size of single item.
	MaxItemSizeBytes uint64 `yaml:"max_item_size_bytes"`
}

func (cfg *InMemoryCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Uint64Var(&cfg.MaxSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory cache used to speed up blocks lookups (shared between all tenants).")
	f.Uint64Var(&cfg.MaxItemSizeBytes, prefix+"max-item-size-bytes", uint64(10*units.MiB), "Maximum size in bytes of in-memory cache used to speed up blocks lookups (shared between all tenants).")
}

// Validate the config.
func (cfg *InMemoryCacheConfig) Validate() error {
	return nil
}

func (cfg InMemoryCacheConfig) ToInMemoryCacheConfig() cache.InMemoryCacheConfig {
	return cache.InMemoryCacheConfig{
		MaxSize:     model.Bytes(cfg.MaxSizeBytes),
		MaxItemSize: model.Bytes(cfg.MaxItemSizeBytes),
	}
}
