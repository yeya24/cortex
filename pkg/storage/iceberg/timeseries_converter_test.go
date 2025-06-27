package iceberg

import (
	"bytes"
	"context"
	"testing"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeseriesParquetConverter_ConvertTimeseriesToParquet(t *testing.T) {
	converter := NewTimeseriesParquetConverter()

	// Create test data
	req := &cortexpb.WriteRequest{
		Timeseries: []cortexpb.PreallocTimeseries{
			{
				TimeSeries: &cortexpb.TimeSeries{
					Labels: []cortexpb.LabelAdapter{
						{Name: "__name__", Value: "cpu_usage"},
						{Name: "instance", Value: "localhost:9090"},
						{Name: "job", Value: "prometheus"},
					},
					Samples: []cortexpb.Sample{
						{TimestampMs: 1000, Value: 0.5},
						{TimestampMs: 2000, Value: 0.6},
					},
				},
			},
			{
				TimeSeries: &cortexpb.TimeSeries{
					Labels: []cortexpb.LabelAdapter{
						{Name: "__name__", Value: "memory_usage"},
						{Name: "instance", Value: "localhost:9090"},
					},
					Samples: []cortexpb.Sample{
						{TimestampMs: 1500, Value: 0.8},
						{TimestampMs: 2500, Value: 0.9},
					},
				},
			},
		},
	}

	// Convert to parquet
	var buf bytes.Buffer
	err := converter.ConvertTimeseriesToParquet(context.Background(), req, &buf)
	require.NoError(t, err)

	// Verify the parquet file can be read
	reader := bytes.NewReader(buf.Bytes())
	file, err := parquet.OpenFile(reader, int64(buf.Len()))
	require.NoError(t, err)
	defer file.Close()

	// Check schema
	schema := file.Schema()
	assert.Equal(t, 4, len(schema.Fields()))

	// Verify column names
	fieldNames := make([]string, 0, 4)
	for _, field := range schema.Fields() {
		fieldNames = append(fieldNames, field.Name())
	}
	expectedFields := []string{"metric_name", "timestamp", "value", "labels"}
	assert.Equal(t, expectedFields, fieldNames)

	// Read and verify data
	rows := make([]map[string]interface{}, 0)
	err = file.ReadRows(func(row parquet.Row) error {
		rowMap := make(map[string]interface{})
		for _, value := range row {
			rowMap[value.Column()] = value.Value()
		}
		rows = append(rows, rowMap)
		return nil
	})
	require.NoError(t, err)

	// Should have 4 rows (2 samples from each timeseries)
	assert.Equal(t, 4, len(rows))

	// Verify data is sorted by metric_name, then timestamp
	expectedOrder := []string{"cpu_usage", "cpu_usage", "memory_usage", "memory_usage"}
	for i, row := range rows {
		assert.Equal(t, expectedOrder[i], row["metric_name"])
	}
}

func TestTimeseriesParquetConverter_ExtractMetricName(t *testing.T) {
	converter := NewTimeseriesParquetConverter()

	labels := []cortexpb.LabelAdapter{
		{Name: "instance", Value: "localhost:9090"},
		{Name: "__name__", Value: "cpu_usage"},
		{Name: "job", Value: "prometheus"},
	}

	metricName := converter.extractMetricName(labels)
	assert.Equal(t, "cpu_usage", metricName)
}

func TestTimeseriesParquetConverter_ExtractMetricName_NotFound(t *testing.T) {
	converter := NewTimeseriesParquetConverter()

	labels := []cortexpb.LabelAdapter{
		{Name: "instance", Value: "localhost:9090"},
		{Name: "job", Value: "prometheus"},
	}

	metricName := converter.extractMetricName(labels)
	assert.Equal(t, "", metricName)
}

func TestTimeseriesParquetConverter_LabelsToJSON(t *testing.T) {
	converter := NewTimeseriesParquetConverter()

	labels := []cortexpb.LabelAdapter{
		{Name: "__name__", Value: "cpu_usage"},
		{Name: "instance", Value: "localhost:9090"},
		{Name: "job", Value: "prometheus"},
	}

	json, err := converter.labelsToJSON(labels)
	require.NoError(t, err)

	// Should not contain __name__ label (it's dropped)
	assert.NotContains(t, string(json), "__name__")
	assert.Contains(t, string(json), "instance")
	assert.Contains(t, string(json), "job")
}
