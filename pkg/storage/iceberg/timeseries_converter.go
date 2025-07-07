package iceberg

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cortexproject/cortex/pkg/storage/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/prometheus/prometheus/model/labels"
	"io"
	"os"
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/parquet-go/parquet-go"
)

// TimeseriesParquetConverter converts Prometheus time series to Parquet format
type TimeseriesParquetConverter struct {
	schema *dynparquet.Schema
}

// NewTimeseriesParquetConverter creates a new converter instance
func NewTimeseriesParquetConverter() *TimeseriesParquetConverter {
	s, err := frostdb.Schema()
	if err != nil {
		panic(err)
	}

	return &TimeseriesParquetConverter{
		schema: s,
	}
}

func ToParquetRow(schema *dynparquet.Schema, lbls labels.Labels, s cortexpb.Sample, labelNames []string) parquet.Row {
	// The order of these appends is important. Parquet values must be in the
	// order of the schema and the schema orders columns by their names.

	nameNumber := len(labelNames)
	row := make([]parquet.Value, 0, nameNumber+2)
	colIdxs := make([]int, lbls.Len())
	columnIndex := 0
	for _, column := range schema.Columns() {
		switch column.Name {
		case frostdb.ColumnTimestamp:
			row = append(row, parquet.ValueOf(s.TimestampMs).Level(0, 0, columnIndex))
			columnIndex++
		case frostdb.ColumnValue:
			row = append(row, parquet.ValueOf(s.Value).Level(0, 0, columnIndex))
			columnIndex++
		case frostdb.ColumnLabels:
			for i, lbl := range labelNames {
				if value := lbls.Get(lbl); value != "" {
					row = append(row, parquet.ValueOf(value).Level(0, 1, columnIndex))
					colIdxs = append(colIdxs, i)
				} else {
					row = append(row, parquet.ValueOf(nil).Level(0, 0, columnIndex))
				}
				columnIndex++
			}
		case frostdb.ColumnIndices:
			row = append(row, parquet.ValueOf(EncodeIntSlice(colIdxs)).Level(0, 0, columnIndex))
			columnIndex++
		}
	}

	return row
}

func EncodeIntSlice(s []int) []byte {
	l := make([]byte, binary.MaxVarintLen32)
	r := make([]byte, 0, len(s)*binary.MaxVarintLen32)

	// Sort to compress more efficiently
	slices.Sort(s)

	// size
	n := binary.PutVarint(l[:], int64(len(s)))
	r = append(r, l[:n]...)

	for i := 0; i < len(s); i++ {
		n := binary.PutVarint(l[:], int64(s[i]))
		r = append(r, l[:n]...)
	}

	return r
}

// ConvertTimeseriesToParquet converts a Prometheus WriteRequest to Parquet format
func (tc *TimeseriesParquetConverter) ConvertTimeseriesToParquet(ctx context.Context, req *cortexpb.WriteRequest, writer io.Writer) error {
	if req == nil || len(req.Timeseries) == 0 {
		return fmt.Errorf("empty or nil write request")
	}

	// Calculate total number of samples
	totalSamples := 0
	labelNames := make(map[string]struct{})
	rows := make([]parquet.Row, 0, len(req.Timeseries))
	for _, ts := range req.Timeseries {
		if len(ts.Samples) == 0 {
			continue
		}
		totalSamples += len(ts.Samples)
		for _, lbl := range ts.Labels {
			if lbl.Name == labels.MetricName {
				continue
			}
			labelNames[lbl.Name] = struct{}{}
		}
	}
	if totalSamples == 0 {
		return fmt.Errorf("no samples in timeseries")
	}
	lbls := make([]string, 0, len(labelNames))
	for lbl := range labelNames {
		lbls = append(lbls, lbl)
	}
	sort.Strings(lbls)
	lbls = append([]string{labels.MetricName}, lbls...)
	for _, ts := range req.Timeseries {
		if len(ts.Samples) == 0 {
			continue
		}
		seriesLabels := cortexpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels)
		for _, sample := range ts.Samples {
			rows = append(rows, ToParquetRow(tc.schema, seriesLabels, sample, lbls))
		}
	}

	//f, err := os.OpenFile("test.parquet", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	//if err != nil {
	//	return err
	//}

	pw, err := tc.schema.GetWriter(writer, map[string][]string{"labels": lbls}, true)
	if err != nil {
		return err
	}

	n, err := pw.WriteRows(rows)
	if err != nil {
		return err
	}
	_ = n

	//// Prepare data structures
	//metricNames := make([]string, 0, totalSamples)
	//timestamps := make([]int64, 0, totalSamples)
	//values := make([]float64, 0, totalSamples)
	//labels := make([][]byte, 0, totalSamples)
	//
	//// Process each timeseries
	//for _, ts := range req.Timeseries {
	//	// Extract metric name from labels
	//	metricName := tc.extractMetricName(ts.Labels)
	//	if metricName == "" {
	//		continue // Skip timeseries without metric name
	//	}
	//
	//	// Convert labels to JSON string
	//	labelsJSON, err := tc.labelsToJSON(ts.Labels)
	//	if err != nil {
	//		return fmt.Errorf("failed to convert labels to JSON: %w", err)
	//	}
	//
	//	// Process each sample
	//	for _, sample := range ts.Samples {
	//		metricNames = append(metricNames, metricName)
	//		timestamps = append(timestamps, sample.TimestampMs)
	//		values = append(values, sample.Value)
	//		labels = append(labels, labelsJSON)
	//	}
	//}
	//
	//// Create a slice of indices for sorting
	//indices := make([]int, len(metricNames))
	//for i := range indices {
	//	indices[i] = i
	//}
	//
	//// Sort indices by metric_name first, then by timestamp
	//sort.Slice(indices, func(i, j int) bool {
	//	if metricNames[indices[i]] != metricNames[indices[j]] {
	//		return metricNames[indices[i]] < metricNames[indices[j]]
	//	}
	//	return timestamps[indices[i]] < timestamps[indices[j]]
	//})
	//
	//// Write data to parquet file in sorted order
	//for _, idx := range indices {
	//	row := map[string]interface{}{
	//		"metric_name": metricNames[idx],
	//		"timestamp":   timestamps[idx],
	//		"value":       values[idx],
	//		"labels":      labels[idx],
	//	}
	//	if err := pw.Write(row); err != nil {
	//		return fmt.Errorf("failed to write row %d: %w", idx, err)
	//	}
	//}

	return pw.Close()
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
