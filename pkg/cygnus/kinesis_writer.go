package cygnus

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util/backoff"
)

const (
	// 900 Kib. Maximum Kinesis record size is 1MiB. Adding extra buffer into calculation for record overhead.
	// https://code.amazon.com/packages/AWSLogsLakeStorageIngestion/blobs/e5b27a04ef4da978f37bfc10594961e80840b8bc/--/src/main/java/com/amazonaws/logs/lake/ingestion/writer/KinesisDataFileEntryWriter.java#L31
	S3LargePayloadThreshold = 921600

	//https://code.amazon.com/packages/AWSLogsLakeStorageIngestion/blobs/e5b27a04ef4da978f37bfc10594961e80840b8bc/--/src/main/java/com/amazonaws/logs/lake/ingestion/writer/KinesisDataFileEntryWriter.java#L32
	DefaultDistributionFactor = 4
)

// DataFileEntryWriter interface for publishing DataFileV1 records
// This interface matches the Java DataFileEntryWriter interface
type DataFileEntryWriter interface {
	Publish(
		ctx context.Context,
		dataFileEntryRecords []*DataFileV1,
		tenantID string,
	) error
}

// KinesisMetrics holds all metrics for Kinesis operations
type KinesisMetrics struct {
	writeLatency       *prometheus.HistogramVec
	attempts           *prometheus.CounterVec
	success            *prometheus.CounterVec
	failedRecords      *prometheus.CounterVec
	largePayloads      *prometheus.CounterVec
	batchWriteFailures *prometheus.CounterVec
}

// NewKinesisMetrics creates a new KinesisMetrics instance
func NewKinesisMetrics(reg prometheus.Registerer) *KinesisMetrics {
	metrics := &KinesisMetrics{
		writeLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kinesis_write_latency_seconds",
			Help:    "Time taken to write records to Kinesis",
			Buckets: prometheus.DefBuckets,
		}, []string{"stream_name"}),

		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kinesis_write_attempts_total",
			Help: "Total number of Kinesis write attempts",
		}, []string{"stream_name"}),

		success: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kinesis_write_success_total",
			Help: "Total number of successful Kinesis writes",
		}, []string{"stream_name"}),

		failedRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kinesis_failed_records_total",
			Help: "Total number of failed Kinesis records",
		}, []string{"stream_name"}),

		largePayloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kinesis_large_payloads_total",
			Help: "Total number of large payloads sent to S3",
		}, []string{"stream_name"}),

		batchWriteFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kinesis_batch_write_failures_total",
			Help: "Total number of Kinesis batch write failures",
		}, []string{"stream_name"}),
	}

	// Register metrics with the provided registry
	if reg != nil {
		reg.MustRegister(
			metrics.writeLatency,
			metrics.attempts,
			metrics.success,
			metrics.failedRecords,
			metrics.largePayloads,
			metrics.batchWriteFailures,
		)
	}

	return metrics
}

// S3LargePayloadKey format for large payload storage
const S3LargePayloadKey = "%s-%s/%s"

// LargePayloadStore interface for storing large payloads in S3
type LargePayloadStore interface {
	Write(ctx context.Context, key string, data []byte) error
}

// KinesisWriter provides a high-level interface for writing to Kinesis
// Implements DataFileEntryWriter interface
type KinesisWriter struct {
	kinesisClient                  *kinesis.Client
	streamName                     string
	dataFileEntryLargePayloadStore LargePayloadStore
	maxRetries                     int
	backoffConfig                  backoff.Config
	metrics                        *KinesisMetrics
}

// NewKinesisWriter creates a new Kinesis writer
func NewKinesisWriter(
	kinesisClient *kinesis.Client,
	streamName string,
	dataFileEntryLargePayloadStore LargePayloadStore,
	maxRetries int,
	metrics *KinesisMetrics,
) *KinesisWriter {
	return &KinesisWriter{
		kinesisClient:                  kinesisClient,
		streamName:                     streamName,
		dataFileEntryLargePayloadStore: dataFileEntryLargePayloadStore,
		maxRetries:                     maxRetries,
		backoffConfig: backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 10 * time.Second,
			MaxRetries: maxRetries,
		},
		metrics: metrics,
	}
}

// Publish implements the DataFileEntryWriter interface
func (k *KinesisWriter) Publish(
	ctx context.Context,
	dataFileEntryRecords []*DataFileV1,
	tenantID string,
) error {
	if len(dataFileEntryRecords) == 0 {
		return nil
	}

	// Create backoff instance
	b := backoff.New(ctx, k.backoffConfig)

	for b.Ongoing() {
		success, err := k.executeWithRetry(ctx, dataFileEntryRecords, tenantID, b.NumRetries())
		if err != nil {
			if !b.Ongoing() {
				return fmt.Errorf("failed to write batch after %d attempts: %w", b.NumRetries(), err)
			}
			b.Wait()
			continue
		}

		if success {
			return nil
		}

		if b.Ongoing() {
			b.Wait()
		}
	}

	return b.Err()
}

// executeWithRetry performs the actual writing logic
func (k *KinesisWriter) executeWithRetry(
	ctx context.Context,
	dataFileEntryRecords []*DataFileV1,
	tenantID string,
	attemptNumber int,
) (bool, error) {
	startTime := time.Now()

	entries, err := k.createPayload(ctx, dataFileEntryRecords, tenantID, attemptNumber)
	if err != nil {
		k.metrics.batchWriteFailures.WithLabelValues(k.streamName).Inc()
		return false, fmt.Errorf("failed to create payload: %w", err)
	}

	putRecordsResult, err := k.putRecords(ctx, entries)
	if err != nil {
		k.metrics.batchWriteFailures.WithLabelValues(k.streamName).Inc()
		return false, fmt.Errorf("failed to put records: %w", err)
	}

	k.emitMetrics(putRecordsResult, startTime)

	failedRecordCount := aws.ToInt32(putRecordsResult.FailedRecordCount)
	if failedRecordCount > 0 {
		return false, nil
	}

	return true, nil
}

// createPayload creates the Kinesis records from DataFileV1 payloads
func (k *KinesisWriter) createPayload(ctx context.Context, dataFileEntryRecords []*DataFileV1, tenantID string, attemptNumber int) ([]types.PutRecordsRequestEntry, error) {
	records := make([]types.PutRecordsRequestEntry, 0, len(dataFileEntryRecords))

	for _, dataFileV1 := range dataFileEntryRecords {
		// Create DataFileInfo
		dataFileInfo := &DataFileInfo{
			FilePath:     dataFileV1.FilePath,
			CreationTime: uint64(time.Now().UnixMilli()),
			AccountId:    tenantID,
		}

		// Create StandardDataFileEntry
		standardDataFile := &StandardDataFileEntry{
			Version:    StandardDataFileEntryVersionV1,
			DataFileV1: dataFileV1,
		}

		// Create DataFileEntry
		dataFileEntry := &DataFileEntry{
			Type:                  DataFileEntryTypeStandard,
			StandardDataFileEntry: standardDataFile,
		}

		// Check if payload is too large
		serializedSize := dataFileV1.Size()
		if serializedSize > S3LargePayloadThreshold {
			k.metrics.largePayloads.WithLabelValues(k.streamName).Inc()

			// Create large payload entry
			s3Key := k.getS3Key(tenantID)
			largeDataFileEntry := &LargeDataFileEntry{
				Key: s3Key,
			}

			dataFileEntry.Type = DataFileEntryTypeLarge
			dataFileEntry.LargeDataFileEntry = largeDataFileEntry

			// Store large payload in S3
			if err := k.putLargeRecord(ctx, s3Key, dataFileEntry); err != nil {
				return nil, fmt.Errorf("failed to store large payload: %w", err)
			}
		}

		// Create DataFileEntryRecord
		record := &DataFileEntryRecord{
			DataFileEntry: dataFileEntry,
			DataFileInfo:  dataFileInfo,
		}

		// Serialize and compress the record
		serialized, err := record.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize record: %w", err)
		}

		compressed, err := k.deflate(serialized)
		if err != nil {
			return nil, fmt.Errorf("failed to compress record: %w", err)
		}

		// Create Kinesis record
		kinesisRecord := types.PutRecordsRequestEntry{
			Data:         compressed,
			PartitionKey: aws.String(k.getPartitionKey(tenantID, attemptNumber)),
		}

		records = append(records, kinesisRecord)
	}

	return records, nil
}

// putRecords sends records to Kinesis
func (k *KinesisWriter) putRecords(ctx context.Context, records []types.PutRecordsRequestEntry) (*kinesis.PutRecordsOutput, error) {
	input := &kinesis.PutRecordsInput{
		StreamName: aws.String(k.streamName),
		Records:    records,
	}

	return k.kinesisClient.PutRecords(ctx, input)
}

// putLargeRecord stores a large payload in S3
func (k *KinesisWriter) putLargeRecord(ctx context.Context, key string, dataFileEntry *DataFileEntry) error {
	serialized, err := dataFileEntry.Marshal()
	if err != nil {
		return fmt.Errorf("failed to serialize large data file entry: %w", err)
	}

	return k.dataFileEntryLargePayloadStore.Write(ctx, key, serialized)
}

// emitMetrics emits metrics for monitoring
func (k *KinesisWriter) emitMetrics(putRecordsResult *kinesis.PutRecordsOutput, startTime time.Time) {
	latency := time.Since(startTime).Seconds()
	k.metrics.writeLatency.WithLabelValues(k.streamName).Observe(latency)

	k.metrics.attempts.WithLabelValues(k.streamName).Inc()

	failedRecordCount := aws.ToInt32(putRecordsResult.FailedRecordCount)
	if failedRecordCount == 0 {
		k.metrics.success.WithLabelValues(k.streamName).Inc()
	} else {
		k.metrics.failedRecords.WithLabelValues(k.streamName).Add(float64(failedRecordCount))
	}
}

// getPartitionKey generates a partition key for Kinesis
func (k *KinesisWriter) getPartitionKey(tenantID string, retryAttempt int) string {
	distributionFactor := int(math.Max(1, float64(DefaultDistributionFactor))) // safeguard against distr < 1
	return fmt.Sprintf("%s-%d-%d", tenantID, distributionFactor, retryAttempt)
}

// getS3Key generates an S3 key for large payload storage
func (k *KinesisWriter) getS3Key(tenantID string) string {
	return fmt.Sprintf(S3LargePayloadKey, tenantID, k.streamName, uuid.New().String())
}

// deflate compresses data using GZIP
func (k *KinesisWriter) deflate(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)

	if _, err := gzipWriter.Write(data); err != nil {
		return nil, err
	}

	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
