package cygnus

import (
	"context"
	fmt "fmt"
	"sync"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

type TableReader interface {
}

type CygnusTableReader struct {
	mtx   sync.RWMutex
	table *table.Table
	fs    iceio.IO
}

func NewCygnusTableReader(tableName string, table *table.Table) *CygnusTableReader {
	return &CygnusTableReader{
		table: table,
	}
}

func (r *CygnusTableReader) refresh(ctx context.Context) error {
	r.mtx.Lock()

	if err := r.table.Refresh(ctx); err != nil {
		r.mtx.Unlock()
		return fmt.Errorf("failed to refresh table: %w", err)
	}
	r.mtx.Unlock()

	fs, err := iceio.LoadFS(ctx, nil, r.table.MetadataLocation())
	if err != nil {
		return fmt.Errorf("failed to load file system for table: %w", err)
	}

	r.fs = fs
	return nil
}
