package cygnus

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
)

// ExampleUsage demonstrates how to use the Kinesis writer
func ExampleUsage() {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("unable to load SDK config", err)
	}

	// Create Kinesis client
	kinesisClient := kinesis.NewFromConfig(cfg)

	// Create S3 client for large payload storage
	s3Client := s3.NewFromConfig(cfg)

	// Create S3 large payload store
	s3LargePayloadStore := NewS3LargePayloadStore(s3Client, "my-large-payload-bucket")

	// Create metrics
	metrics := NewKinesisMetrics(prometheus.DefaultRegisterer)

	// Create Kinesis writer
	writer := NewKinesisWriter(
		kinesisClient,
		"my-kinesis-stream",
		s3LargePayloadStore,
		3, // max retries
		metrics,
	)

	// Create sample DataFileV1 records
	records := []*DataFileV1{
		{
			FilePath:        "/path/to/file1.parquet",
			FileFormat:      FileFormatParquet,
			RecordCount:     1000,
			FileSizeInBytes: 1024,
			ColumnSizes: map[int32]uint64{
				1: 512,
				2: 512,
			},
			ValueCounts: map[int32]uint64{
				1: 1000,
				2: 1000,
			},
			NullValueCounts: map[int32]uint64{
				1: 0,
				2: 0,
			},
			NanValueCounts: map[int32]uint64{
				1: 0,
				2: 0,
			},
			IndexedFields: []string{"field1", "field2"},
		},
		{
			FilePath:        "/path/to/file2.parquet",
			FileFormat:      FileFormatParquet,
			RecordCount:     2000,
			FileSizeInBytes: 2048,
			ColumnSizes: map[int32]uint64{
				1: 1024,
				2: 1024,
			},
			ValueCounts: map[int32]uint64{
				1: 2000,
				2: 2000,
			},
			NullValueCounts: map[int32]uint64{
				1: 0,
				2: 0,
			},
			NanValueCounts: map[int32]uint64{
				1: 0,
				2: 0,
			},
			IndexedFields: []string{"field1", "field2"},
		},
	}

	// Write batch to Kinesis
	ctx := context.Background()
	err = writer.Publish(ctx, records, "my-log-group-id")
	if err != nil {
		log.Fatal("failed to write batch", err)
	}

	log.Println("Successfully wrote batch to Kinesis")
}

// ExampleWithDataFileEntryWriterInterface demonstrates using the DataFileEntryWriter interface
func ExampleWithDataFileEntryWriterInterface() {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("unable to load SDK config", err)
	}

	// Create clients
	kinesisClient := kinesis.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)
	s3LargePayloadStore := NewS3LargePayloadStore(s3Client, "my-large-payload-bucket")

	// Create metrics
	metrics := NewKinesisMetrics(prometheus.DefaultRegisterer)

	// Create Kinesis writer that implements DataFileEntryWriter
	var writer DataFileEntryWriter = NewKinesisWriter(
		kinesisClient,
		"my-kinesis-stream",
		s3LargePayloadStore,
		3, // max retries
		metrics,
	)

	// Create sample DataFileV1 records
	records := []*DataFileV1{
		{
			FilePath:        "/path/to/file1.parquet",
			FileFormat:      FileFormatParquet,
			RecordCount:     1000,
			FileSizeInBytes: 1024,
			ColumnSizes: map[int32]uint64{
				1: 512,
				2: 512,
			},
			ValueCounts: map[int32]uint64{
				1: 1000,
				2: 1000,
			},
			NullValueCounts: map[int32]uint64{
				1: 0,
				2: 0,
			},
			NanValueCounts: map[int32]uint64{
				1: 0,
				2: 0,
			},
			IndexedFields: []string{"field1", "field2"},
		},
	}

	// Publish using the interface
	ctx := context.Background()
	err = writer.Publish(
		ctx,
		records,
		"my-log-group-id",
	)
	if err != nil {
		log.Fatal("failed to publish", err)
	}

	log.Println("Successfully published using DataFileEntryWriter interface")
}

// ExampleWithCustomRetry demonstrates custom retry logic
func ExampleWithCustomRetry() {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("unable to load SDK config", err)
	}

	// Create clients
	kinesisClient := kinesis.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)
	s3LargePayloadStore := NewS3LargePayloadStore(s3Client, "my-large-payload-bucket")

	// Create metrics
	metrics := NewKinesisMetrics(prometheus.DefaultRegisterer)

	// Create Kinesis writer with custom retry configuration
	writer := NewKinesisWriter(
		kinesisClient,
		"my-kinesis-stream",
		s3LargePayloadStore,
		5, // max retries
		metrics,
	)

	// Create sample records
	records := []*DataFileV1{
		{
			FilePath:        "/path/to/file1.parquet",
			FileFormat:      FileFormatParquet,
			RecordCount:     1000,
			FileSizeInBytes: 1024,
		},
	}

	// Execute with custom retry logic
	ctx := context.Background()
	err = writer.Publish(ctx, records, "my-log-group-id")
	if err != nil {
		log.Fatal("failed to execute", err)
	}

	log.Println("Successfully executed with custom retry configuration")
}
