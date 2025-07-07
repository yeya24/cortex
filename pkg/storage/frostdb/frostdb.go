package frostdb

import (
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

const (
	SchemaName = "cortex"
	// The columns are sorted by their name in the schema too.
	ColumnLabels = "labels"
	//ColumnMetricName = "metric_name"
	ColumnIndices   = "s_col_indexes"
	ColumnTimestamp = "timestamp"
	ColumnValue     = "value"
)

func SchemaDefinition() *schemapb.Schema {
	return &schemapb.Schema{
		Name: SchemaName,
		Columns: []*schemapb.Column{
			//{
			//	Name: ColumnMetricName,
			//	StorageLayout: &schemapb.StorageLayout{
			//		Type:     schemapb.StorageLayout_TYPE_STRING,
			//		Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			//	},
			//	Dynamic: false,
			//},
			{
				Name: ColumnLabels,
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Nullable: true,
				},
				Dynamic: true,
			}, {
				Name: ColumnTimestamp,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_INT64,
					Encoding:    schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
				},
				Dynamic: false,
			}, {
				Name: ColumnValue,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_DOUBLE,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
				},
				Dynamic: false,
			},
			{
				Name: ColumnIndices,
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_DELTA_BYTE_ARRAY,
				},
				Dynamic: false,
			},
		},
		SortingColumns: []*schemapb.SortingColumn{
			{
				Name:      ColumnLabels,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			}, {
				Name:      ColumnTimestamp,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
		},
	}
}

func Schema() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(SchemaDefinition())
}
