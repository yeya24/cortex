package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"

	"github.com/apache/iceberg-go/io"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/go-kit/log"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/thanos-io/objstore"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/iceberg-go/catalog/glue"

	"github.com/apache/arrow-go/v18/arrow"
)

// mockDataFile is a mock implementation of iceberg.DataFile for testing
type mockDataFile struct {
	lowerBounds map[int][]byte
	upperBounds map[int][]byte
}

func (m *mockDataFile) LowerBoundValues() map[int][]byte {
	return m.lowerBounds
}

func (m *mockDataFile) UpperBoundValues() map[int][]byte {
	return m.upperBounds
}

// Implement other required methods with empty implementations for testing
func (m *mockDataFile) ContentType() iceberg.ManifestEntryContent { return iceberg.EntryContentData }
func (m *mockDataFile) FilePath() string                          { return "" }
func (m *mockDataFile) FileFormat() iceberg.FileFormat            { return iceberg.ParquetFile }
func (m *mockDataFile) RecordCount() int64                        { return 0 }
func (m *mockDataFile) FileSizeBytes() int64                      { return 0 }
func (m *mockDataFile) ColumnSizes() map[int]int64                { return nil }
func (m *mockDataFile) ValueCounts() map[int]int64                { return nil }
func (m *mockDataFile) NullValueCounts() map[int]int64            { return nil }
func (m *mockDataFile) NaNValueCounts() map[int]int64             { return nil }
func (m *mockDataFile) SplitOffsets() []int64                     { return nil }
func (m *mockDataFile) KeyMetadata() []byte                       { return nil }
func (m *mockDataFile) SortOrderID() *int                         { return nil }
func (m *mockDataFile) EqualityFieldIDs() []int                   { return nil }
func (m *mockDataFile) Partition() map[string]any                 { return nil }
func (m *mockDataFile) Count() int64                              { return 0 }

func TestIcebergGlue_ListAndCreateOperations(t *testing.T) {
	// Skip if not running in AWS environment with proper credentials
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Initialize the Iceberg Glue catalog
	cfg := IcebergConfig{
		Backend: "rest",
		Rest: RestConfig{
			URI:               "https://s3tables.us-west-2.amazonaws.com/iceberg",
			WarehouseLocation: "arn:aws:s3tables:us-west-2:029178969873:bucket/cortex-block-storage-benye-iceberg-029178969873",
		},
		Namespace: "iceberg",
	}
	logger := log.NewNopLogger()
	var (
		bkt objstore.Bucket
		err error
	)
	//bkt = NewIcebucket("s3://cortex-block-storage-benye-iceberg-029178969873/", bkt)
	icebergGlue, err := NewIcebergStore(cfg)
	require.NoError(t, err, "Failed to create Iceberg Glue catalog")
	require.NotNil(t, icebergGlue, "Iceberg Glue catalog should not be nil")

	// Get the underlying catalog
	ctlg := icebergGlue.GetCatalog()
	require.NotNil(t, ctlg, "Underlying catalog should not be nil")

	t.Run("CreateNamespace", func(t *testing.T) {
		di := glue.DatabaseIdentifier("iceberg")
		err = ctlg.CreateNamespace(ctx, di, nil)
		require.NoError(t, err)

		ns, err := ctlg.ListNamespaces(ctx, nil)
		require.NoError(t, err)
		found := false
		for _, n := range ns {
			if n[0] == "iceberg" {
				found = true
				break
			}
		}
		require.True(t, found)
	})

	// Test 1: List tables (should work even if no tables exist)
	t.Run("ListTables", func(t *testing.T) {
		tbls := make([]table.Identifier, 0)
		iter := ctlg.ListTables(ctx, glue.DatabaseIdentifier("iceberg"))
		for tbl, err := range iter {
			tbls = append(tbls, tbl)
			assert.NoError(t, err)
		}

		fmt.Println(tbls)
	})

	t.Run("ListNamespaces", func(t *testing.T) {
		namespaces, err := ctlg.ListNamespaces(ctx, nil)
		require.NoError(t, err, "ListNamespaces should not return an error")
		for _, namespace := range namespaces {
			fmt.Println(namespace)
		}
	})

	t.Run("DropTable", func(t *testing.T) {
		tbIdentifier := glue.TableIdentifier("iceberg", "test_table10")
		err = ctlg.(*rest.Catalog).PurgeTable(ctx, tbIdentifier)
		require.NoError(t, err)
	})

	// Test 2: Create a simple table
	t.Run("CreateTable", func(t *testing.T) {
		// Create a simple schema for testing
		schema := tableSchema()

		// Create partition spec (no partitioning for simplicity)
		partitionSpec := iceberg.NewPartitionSpec()

		// Table properties
		properties := map[string]string{
			"write.format.default":                    "parquet",
			"write.parquet.compression-codec":         "zstd",
			table.MetadataDeleteAfterCommitEnabledKey: "true",
			table.MetadataPreviousVersionsMaxKey:      "3",
			table.ManifestMergeEnabledKey:             "false",
			table.ManifestMinMergeCountKey:            "2",
		}

		// Generate unique table name to avoid conflicts
		tableName := "test_table10"

		tbIdentifier := glue.TableIdentifier("iceberg", tableName)
		createOpts := []catalog.CreateTableOpt{
			catalog.WithLocation("s3://cortex-block-storage-benye-iceberg-029178969873"),
			catalog.WithProperties(properties),
			catalog.WithPartitionSpec(&partitionSpec),
		}
		// Create the table
		table, err := ctlg.CreateTable(ctx, tbIdentifier, schema, createOpts...)
		require.NoError(t, err, "CreateTable should not return an error")
		require.NotNil(t, table, "Created table should not be nil")

		t.Logf("Successfully created table: %s", tableName)

		// Verify the table was created by listing tables again
		tables := ctlg.ListTables(ctx, glue.DatabaseIdentifier("iceberg"))
		require.NoError(t, err, "ListTables after creation should not return an error")

		// Check if our new table is in the list
		found := false
		for tbl, err := range tables {
			require.NoError(t, err)
			if tbl[1] == tableName {
				found = true
				break
			}
		}
		assert.True(t, found, "Created table should be found in the table list")
	})

	// 	// Test 3: Load the created table
	// 	t.Run("LoadTable", func(t *testing.T) {
	// 		loadedTable, err := catalog.LoadTable(ctx, tableName)
	// 		require.NoError(t, err, "LoadTable should not return an error")
	// 		require.NotNil(t, loadedTable, "Loaded table should not be nil")

	// 		// Verify table properties
	// 		assert.Equal(t, tableName, loadedTable.Name(), "Table name should match")

	// 		// Verify schema
	// 		loadedSchema := loadedTable.Schema()
	// 		require.NotNil(t, loadedSchema, "Table schema should not be nil")
	// 		assert.Equal(t, testSchema.NumColumns(), loadedSchema.NumColumns(), "Schema should have same number of columns")

	// 		t.Logf("Successfully loaded table: %s", loadedTable.Name())
	// 	})

	// 	// Test 4: Drop the created table (cleanup)
	// 	t.Run("DropTable", func(t *testing.T) {
	// 		err := catalog.DropTable(ctx, tableName)
	// 		require.NoError(t, err, "DropTable should not return an error")

	// 		t.Logf("Successfully dropped table: %s", tableName)

	// 		// Verify the table was dropped by trying to load it
	// 		_, err = catalog.LoadTable(ctx, tableName)
	// 		assert.Error(t, err, "LoadTable should return an error for dropped table")
	// 	})

	t.Run("LoadTable", func(t *testing.T) {
		tbl, err := ctlg.LoadTable(ctx, glue.TableIdentifier("iceberg", "test_table10"), nil)
		require.NoError(t, err)
		require.NotNil(t, tbl)
		schemaMap := tbl.Schemas()
		fmt.Printf("%+v\n", schemaMap)
		schema := tbl.Schema()
		require.NotNil(t, schema)
		fmt.Println(schema)
		id := tbl.Identifier()
		require.NotNil(t, id)
		fmt.Println(id)
		properties := tbl.Properties()
		require.NotNil(t, properties)
		fmt.Println(properties)
		location := tbl.Location()
		require.NotNil(t, location)
		fmt.Println(location)
		iter := tbl.AllManifests(ctx)
		for manifest, err := range iter {
			require.NoError(t, err)
			fmt.Println(manifest)
		}
		sp := tbl.CurrentSnapshot()
		loc := tbl.MetadataLocation()
		u, err := url.Parse(loc)
		require.NoError(t, err)
		bkt, err = bucket.NewClient(ctx, bucket.Config{
			Backend: "s3",
			S3: s3.Config{
				Endpoint:         "s3tables.us-west-2.amazonaws.com",
				BucketName:       u.Host,
				Region:           "us-west-2",
				BucketLookupType: s3.BucketAutoLookup,
			},
		}, nil, "iceberg", logger, nil)
		bkt = NewIcebucket("s3://"+u.Host+"/", bkt)
		require.NoError(t, err)
		fs, err := io.LoadFS(ctx, nil, loc)
		require.NoError(t, err)
		mf, err := sp.Manifests(fs)
		require.NoError(t, err)
		for _, manifest := range mf {
			me, err := manifest.FetchEntries(fs, true)
			require.NoError(t, err)
			for _, entry := range me {
				path := entry.DataFile().FilePath()
				fmt.Println(path)
				r := storage.NewBucketReadAt(ctx, path, bkt)
				uppers := entry.DataFile().UpperBoundValues()
				lowers := entry.DataFile().LowerBoundValues()
				fmt.Println(getTimestamp(uppers[0]))
				fmt.Println(getTimestamp(lowers[0]))
				fileOptions := []parquet.FileOption{
					parquet.SkipMagicBytes(true),
					parquet.ReadBufferSize(100 * 1024),
					parquet.SkipBloomFilters(true),
				}
				size := entry.DataFile().FileSizeBytes()
				f, err := parquet.OpenFile(r, size, fileOptions...)
				require.NoError(t, err)
				for _, rg := range f.RowGroups() {
					fmt.Println(rg.NumRows())
					fmt.Println(rg.Rows())
				}
			}
		}
	})

	t.Run("AppendData", func(t *testing.T) {
		tbl, err := ctlg.LoadTable(ctx, glue.TableIdentifier("iceberg", "test_table10"), nil)
		require.NoError(t, err)
		arrTbl, err := arrowTable()
		require.NoError(t, err)
		// Table properties
		properties := map[string]string{
			"write.format.default":                    "parquet",
			"write.parquet.compression-codec":         "zstd",
			table.MetadataDeleteAfterCommitEnabledKey: "true",
			table.MetadataPreviousVersionsMaxKey:      "3",
			table.ManifestMergeEnabledKey:             "false",
			table.ManifestMinMergeCountKey:            "2",
		}
		tbl, err = tbl.AppendTable(ctx, arrTbl, 10, properties)
		require.NoError(t, err)
		fmt.Println("Appended data")
	})
}

func tableSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "timestamp", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "metric_name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "Value", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 4, Name: "labels", Type: iceberg.PrimitiveTypes.Binary, Required: false},
	)
}

func arrowTable() (arrow.Table, error) {
	// Create Arrow schema directly
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "metric_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "Value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "labels", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	// Create builders
	timestampBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	metricNameBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	valueBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	labelsBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)

	defer timestampBuilder.Release()
	defer metricNameBuilder.Release()
	defer valueBuilder.Release()
	defer labelsBuilder.Release()

	// Add sample data
	timestamps := []int64{1640995200000, 1640995260000, 1640995320000, 1640995380000}
	metricNames := []string{"http_requests_total", "http_requests_total", "cpu_usage", "cpu_usage"}
	values := []float64{100.5, 102.3, 75.2, 78.9}

	// Sample labels as JSON binary
	labelsData := [][]byte{
		[]byte(`{"job":"web","instance":"server1"}`),
		[]byte(`{"job":"web","instance":"server2"}`),
		[]byte(`{"job":"monitoring","instance":"server1"}`),
		[]byte(`{"job":"monitoring","instance":"server2"}`),
	}

	// Append data to builders
	for i := 0; i < len(timestamps); i++ {
		timestampBuilder.Append(timestamps[i])
		metricNameBuilder.Append(metricNames[i])
		valueBuilder.Append(values[i])
		labelsBuilder.Append(labelsData[i])
	}

	// Create arrays
	timestampArray := timestampBuilder.NewArray()
	metricNameArray := metricNameBuilder.NewArray()
	valueArray := valueBuilder.NewArray()
	labelsArray := labelsBuilder.NewArray()

	// Create record
	record := array.NewRecord(schema, []arrow.Array{
		timestampArray,
		metricNameArray,
		valueArray,
		labelsArray,
	}, int64(len(timestamps)))

	// Create table from record
	table := array.NewTableFromRecords(schema, []arrow.Record{record})

	return table, nil
}
