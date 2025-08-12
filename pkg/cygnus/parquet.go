package cygnus

import (
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// ParquetToDataFileV1 extracts metadata from a parquet file and converts it to DataFileV1 format
func ParquetToDataFileV1(pf parquet.FileView, filePath string) *DataFileV1 {
	metadata := pf.Metadata()
	if metadata == nil {
		// Create empty metadata if none exists
		metadata = &format.FileMetaData{
			NumRows: 0,
		}
	}

	// Extract basic file information
	dataFileV1 := &DataFileV1{
		FilePath:         filePath,
		FileFormat:       FileFormatParquet,
		RecordCount:      uint64(metadata.NumRows),
		FileSizeInBytes:  uint64(pf.Size()),
		ColumnSizes:      make(map[int32]uint64),
		ValueCounts:      make(map[int32]uint64),
		NullValueCounts:  make(map[int32]uint64),
		NanValueCounts:   make(map[int32]uint64),
		LowerBounds:      make(map[int32][]byte),
		UpperBounds:      make(map[int32][]byte),
		IndexedFields:    []string{},
		SchemaReferences: []*SchemaReference{},
	}

	// Track min/max values for each column across all row groups
	columnBounds := make(map[int32]struct {
		min parquet.Value
		max parquet.Value
	})

	fileMetadata := pf.Metadata()
	schema := pf.Schema()
	for _, rowGroup := range fileMetadata.RowGroups {
		for _, column := range rowGroup.Columns {
			col, ok := schema.Lookup(column.MetaData.PathInSchema...)
			if !ok {
				continue
			}
			columnID := int32(col.ColumnIndex)
			dataFileV1.ColumnSizes[columnID] += uint64(column.MetaData.TotalCompressedSize)
		}
	}

	// Extract column metadata from row groups
	for _, rowGroup := range pf.RowGroups() {
		for _, columnChunk := range rowGroup.ColumnChunks() {
			path := rowGroup.Schema().Columns()[columnChunk.Column()]
			col, ok := schema.Lookup(path...)
			if !ok {
				continue
			}
			columnID := int32(col.ColumnIndex)

			// Get column metadata
			column := columnChunk.(*parquet.FileColumnChunk)

			// Extract value counts
			dataFileV1.ValueCounts[columnID] += uint64(column.NumValues())

			// Extract null value counts
			dataFileV1.NullValueCounts[columnID] += uint64(column.NullCount())

			// Get bounds using the proper parquet API
			if min, max, ok := column.Bounds(); ok {
				if bounds, exists := columnBounds[columnID]; !exists {
					// First occurrence, set initial bounds
					columnBounds[columnID] = struct {
						min parquet.Value
						max parquet.Value
					}{
						min: min,
						max: max,
					}
				} else {
					// Update bounds by comparing with existing values using parquet Type comparison
					columnType := column.Type()
					if columnType.Compare(min, bounds.min) < 0 {
						bounds.min = min
					}
					if columnType.Compare(max, bounds.max) > 0 {
						bounds.max = max
					}
					columnBounds[columnID] = bounds
				}
			}
		}
	}

	// Set the final aggregated bounds by converting Value to bytes
	for columnID, bounds := range columnBounds {
		// Convert min value to bytes
		if !bounds.min.IsNull() {
			dataFileV1.LowerBounds[columnID] = bounds.min.Bytes()
		}
		// Convert max value to bytes
		if !bounds.max.IsNull() {
			dataFileV1.UpperBounds[columnID] = bounds.max.Bytes()
		}
	}

	for _, col := range schema.Columns() {
		dataFileV1.IndexedFields = append(dataFileV1.IndexedFields, col[0])
	}

	return dataFileV1
}
