package iceberg

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/apache/iceberg-go"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/parquet-go/parquet-go"
)

// TimeseriesParquetConverter converts Prometheus time series to Parquet format
type TimeseriesParquetConverter struct {
	schema        *parquet.Schema
	writerOptions []parquet.WriterOption
}

// NewTimeseriesParquetConverter creates a new converter instance
func NewTimeseriesParquetConverter() *TimeseriesParquetConverter {
	// Define the parquet schema with metric_name as primary sort and timestamp as secondary
	g := parquet.Group{
		"metric_name": parquet.Required(parquet.Encoded(parquet.String(), &parquet.DeltaByteArray)),
		"timestamp":   parquet.Required(parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked)),
		"value":       parquet.Leaf(parquet.DoubleType),
		"labels":      parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray),
	}
	schema := parquet.NewSchema("timeseries", g)

	// Create writer options with sorting configuration
	writerOptions := []parquet.WriterOption{
		schema,
		parquet.SortingWriterConfig(
			parquet.SortingColumns(
				parquet.Ascending("metric_name"),
				parquet.Ascending("timestamp"),
			),
		),
		parquet.MaxRowsPerRowGroup(10000),    // 1M rows per row group
		parquet.PageBufferSize(1024 * 1024),  // 1MB page buffer
		parquet.WriteBufferSize(1024 * 1024), // 1MB write buffer
	}

	return &TimeseriesParquetConverter{
		schema:        schema,
		writerOptions: writerOptions,
	}
}

// ConvertTimeseriesToParquet converts a Prometheus WriteRequest to Parquet format
func (tc *TimeseriesParquetConverter) ConvertTimeseriesToParquet(ctx context.Context, req *cortexpb.WriteRequest, writer io.Writer) error {
	if req == nil || len(req.Timeseries) == 0 {
		return fmt.Errorf("empty or nil write request")
	}

	// Calculate total number of samples
	totalSamples := 0
	for _, ts := range req.Timeseries {
		totalSamples += len(ts.Samples)
	}

	if totalSamples == 0 {
		return fmt.Errorf("no samples in timeseries")
	}

	// Create parquet writer with configured options
	pw := parquet.NewWriter(writer, tc.writerOptions...)
	defer pw.Close()

	// Prepare data structures
	metricNames := make([]string, 0, totalSamples)
	timestamps := make([]int64, 0, totalSamples)
	values := make([]float64, 0, totalSamples)
	labels := make([][]byte, 0, totalSamples)

	// Process each timeseries
	for _, ts := range req.Timeseries {
		// Extract metric name from labels
		metricName := tc.extractMetricName(ts.Labels)
		if metricName == "" {
			continue // Skip timeseries without metric name
		}

		// Convert labels to JSON string
		labelsJSON, err := tc.labelsToJSON(ts.Labels)
		if err != nil {
			return fmt.Errorf("failed to convert labels to JSON: %w", err)
		}

		// Process each sample
		for _, sample := range ts.Samples {
			metricNames = append(metricNames, metricName)
			timestamps = append(timestamps, sample.TimestampMs)
			values = append(values, sample.Value)
			labels = append(labels, labelsJSON)
		}
	}

	// Create a slice of indices for sorting
	indices := make([]int, len(metricNames))
	for i := range indices {
		indices[i] = i
	}

	// Sort indices by metric_name first, then by timestamp
	sort.Slice(indices, func(i, j int) bool {
		if metricNames[indices[i]] != metricNames[indices[j]] {
			return metricNames[indices[i]] < metricNames[indices[j]]
		}
		return timestamps[indices[i]] < timestamps[indices[j]]
	})

	// Write data to parquet file in sorted order
	for _, idx := range indices {
		row := map[string]interface{}{
			"metric_name": metricNames[idx],
			"timestamp":   timestamps[idx],
			"value":       values[idx],
			"labels":      labels[idx],
		}
		if err := pw.Write(row); err != nil {
			return fmt.Errorf("failed to write row %d: %w", idx, err)
		}
	}

	return nil
}

// ConvertTimeseriesToParquetWithBatching converts timeseries with batching for large datasets
func (tc *TimeseriesParquetConverter) ConvertTimeseriesToParquetWithBatching(ctx context.Context, req *cortexpb.WriteRequest, writer io.Writer, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	totalTimeseries := len(req.Timeseries)

	for i := 0; i < totalTimeseries; i += batchSize {
		end := i + batchSize
		if end > totalTimeseries {
			end = totalTimeseries
		}

		batchReq := &cortexpb.WriteRequest{
			Timeseries: req.Timeseries[i:end],
		}

		if err := tc.ConvertTimeseriesToParquet(ctx, batchReq, writer); err != nil {
			return fmt.Errorf("failed to convert batch %d-%d: %w", i, end, err)
		}
	}

	return nil
}

// extractMetricName extracts the metric name from labels
func (tc *TimeseriesParquetConverter) extractMetricName(labels []cortexpb.LabelAdapter) string {
	for _, label := range labels {
		if label.Name == "__name__" {
			return label.Value
		}
	}
	return ""
}

// labelsToJSON converts labels to a JSON string format
func (tc *TimeseriesParquetConverter) labelsToJSON(labels []cortexpb.LabelAdapter) ([]byte, error) {
	json, err := cortexpb.FromLabelAdaptersToLabels(labels).DropMetricName().MarshalJSON()
	return json, err
}

// GetIcebergSchema returns the Iceberg schema for timeseries data
func (tc *TimeseriesParquetConverter) GetIcebergSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       1,
			Name:     "metric_name",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "timestamp",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "value",
			Type:     iceberg.PrimitiveTypes.Float64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       4,
			Name:     "labels",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: false,
		},
	)
}

// ConvertTimeseriesToParquetFile converts timeseries to parquet and writes to a file
func (tc *TimeseriesParquetConverter) ConvertTimeseriesToParquetFile(ctx context.Context, req *cortexpb.WriteRequest, filename string) error {
	// Create the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create parquet file %s: %w", filename, err)
	}
	defer file.Close()

	// Convert and write to the file
	return tc.ConvertTimeseriesToParquet(ctx, req, file)
}
