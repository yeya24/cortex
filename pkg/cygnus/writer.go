package cygnus

import (
	"context"

	parquet_storage "github.com/prometheus-community/parquet-common/storage"

	"github.com/parquet-go/parquet-go"
)

type CygnusWriter struct {
	writer             DataFileEntryWriter
	parquetFileOptions parquet_storage.FileOption
}

func NewCygnusWriter(writer DataFileEntryWriter) *CygnusWriter {
	return &CygnusWriter{
		writer: writer,
		parquetFileOptions: parquet_storage.WithFileOptions(
			parquet.SkipBloomFilters(true),
		),
	}
}

type Writer interface {
	Write(ctx context.Context, path string, tenantID string) error
}

func (c *CygnusWriter) Write(ctx context.Context, path string, tenantID string) error {
	f, err := parquet_storage.OpenFromFile(ctx, path, c.parquetFileOptions)
	if err != nil {
		return err
	}

	df := ParquetToDataFileV1(f)
	if err := c.writer.Publish(ctx, []*DataFileV1{df}, tenantID); err != nil {
		return err
	}
	return nil
}
