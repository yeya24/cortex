package frostdb

import (
	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

type Config struct {
	Path             string `yaml:"path"`
	ActiveMemorySize int64  `yaml:"active_memory_size"`
	bucket.Config    `yaml:",inline"`
}
