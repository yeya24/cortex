package frostdb

import (
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

const (
	ColumnTenant         = "tenant"
	ColumnLabels         = "labels"
	ColumnExemplarLabels = "exemplar_labels"
	ColumnTimestamp      = "timestamp"
	ColumnValue          = "value"
)

func exemplarSchema() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "exemplars_schema",
		Columns: []*schemapb.Column{
			{
				Name: ColumnTenant,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_STRING,
					Encoding:    schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
					Nullable:    false,
				},
				Dynamic: false,
			},
			{
				Name: ColumnLabels,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_STRING,
					Encoding:    schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
					Nullable:    true,
				},
				Dynamic: true,
			},
			{
				Name: ColumnExemplarLabels,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_STRING,
					Encoding:    schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
					Nullable:    true,
				},
				Dynamic: true,
			},
			{
				Name: ColumnTimestamp,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_INT64,
					Encoding:    schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
				},
				Dynamic: false,
			},
			{
				Name: ColumnValue,
				StorageLayout: &schemapb.StorageLayout{
					Type:        schemapb.StorageLayout_TYPE_DOUBLE,
					Compression: schemapb.StorageLayout_COMPRESSION_LZ4_RAW,
				},
				Dynamic: false,
			},
		},
		SortingColumns: []*schemapb.SortingColumn{
			{
				Name:      ColumnTenant,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:      ColumnTimestamp,
				Direction: schemapb.SortingColumn_DIRECTION_DESCENDING,
			},
			{
				Name:      ColumnLabels,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:      ColumnExemplarLabels,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
		},
	})
}
