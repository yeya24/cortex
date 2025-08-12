# Kinesis Writer for DataFileV1 Records

This package provides a Go implementation of a Kinesis writer that follows the Java implementation pattern. It handles writing `DataFileV1` records to Amazon Kinesis with support for large payloads, compression, retries, and comprehensive metrics.

## Features

- **Batch Writing**: Write multiple `DataFileV1` records in a single batch
- **Large Payload Support**: Automatically store large payloads in S3 when they exceed the threshold
- **GZIP Compression**: Compress records using GZIP before sending to Kinesis
- **Retry Logic**: Built-in retry mechanism with exponential backoff
- **Metrics**: Comprehensive Prometheus metrics for monitoring
- **Partition Key Generation**: Intelligent partition key generation for load distribution
- **Context Support**: Full context support for cancellation and timeouts

## Architecture

The implementation consists of two main components:

1. **KinesisWriterRetryer**: Handles the core logic for creating payloads, writing to Kinesis, and managing retries
2. **KinesisWriter**: High-level interface that provides batch writing with retry logic

### Key Components

- **LargePayloadStore**: Interface for storing large payloads (S3 implementation provided)
- **Metrics**: Prometheus metrics for monitoring write operations
- **Compression**: GZIP compression for efficient data transfer
- **Partition Key Generation**: Smart partition key generation for load distribution

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "log"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/kinesis"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "your-project/pkg/cygnus"
)

func main() {
    // Load AWS configuration
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatal("unable to load SDK config", err)
    }

    // Create clients
    kinesisClient := kinesis.NewFromConfig(cfg)
    s3Client := s3.NewFromConfig(cfg)
    s3LargePayloadStore := cygnus.NewS3LargePayloadStore(s3Client, "my-large-payload-bucket")

    // Create Kinesis writer
    writer := cygnus.NewKinesisWriter(
        kinesisClient,
        "my-kinesis-stream",
        s3LargePayloadStore,
        "my-log-group-id",
        "my-schema-name",
        "123456789012",
        10,    // distribution factor
        1024,  // large payload threshold (1KB)
        3,     // max retries
    )

    // Create DataFileV1 records
    records := []*cygnus.DataFileV1{
        {
            FilePath:        "/path/to/file.parquet",
            FileFormat:      cygnus.FileFormat_Parquet,
            RecordCount:     1000,
            FileSizeInBytes: 1024,
            // ... other fields
        },
    }

    // Write batch to Kinesis
    ctx := context.Background()
    err = writer.WriteBatch(ctx, records)
    if err != nil {
        log.Fatal("failed to write batch", err)
    }

    log.Println("Successfully wrote batch to Kinesis")
}
```

### Advanced Usage with Custom Retry Logic

```go
// Create retryer directly for more control
retryer := cygnus.NewKinesisWriterRetryer(
    kinesisClient,
    "my-kinesis-stream",
    s3LargePayloadStore,
    "my-log-group-id",
    "my-schema-name",
    "123456789012",
    10,   // distribution factor
    1024, // large payload threshold
    records,
)

// Execute with custom retry logic
ctx := context.Background()
success, err := retryer.Execute(ctx)
if err != nil {
    log.Fatal("failed to execute", err)
}

if success {
    log.Println("Successfully executed")
} else {
    log.Println("Execution failed, retry needed")
}
```

## Configuration

### KinesisWriter Parameters

- **kinesisClient**: AWS Kinesis client
- **streamName**: Name of the Kinesis stream
- **dataFileEntryLargePayloadStore**: Implementation of LargePayloadStore interface
- **logGroupId**: Log group identifier
- **schemaName**: Schema name for partition key generation
- **accountId**: AWS account ID
- **distributionFactor**: Factor for load distribution (minimum 1)
- **largePayloadThreshold**: Size threshold in bytes for large payloads
- **maxRetries**: Maximum number of retry attempts

### Large Payload Threshold

When a `DataFileV1` record exceeds the `largePayloadThreshold`, it will be:
1. Stored in S3 with a unique key
2. A reference to the S3 location will be sent to Kinesis instead
3. The S3 key format follows: `{logGroupId}-{schemaName}-{streamName}/{uuid}`

## Metrics

The implementation provides comprehensive Prometheus metrics:

### Counters
- `kinesis_write_attempts_total`: Total number of Kinesis write attempts
- `kinesis_write_success_total`: Total number of successful Kinesis writes
- `kinesis_failed_records_total`: Total number of failed Kinesis records
- `kinesis_large_payloads_total`: Total number of large payloads sent to S3
- `kinesis_batch_write_failures_total`: Total number of Kinesis batch write failures

### Histograms
- `kinesis_write_latency_seconds`: Time taken to write records to Kinesis

### Labels
All metrics include a `stream_name` label for filtering and aggregation.

## Error Handling

The implementation handles various error scenarios:

1. **Serialization Errors**: When protobuf serialization fails
2. **Compression Errors**: When GZIP compression fails
3. **S3 Errors**: When storing large payloads fails
4. **Kinesis Errors**: When writing to Kinesis fails
5. **Retry Logic**: Automatic retry with exponential backoff

## Partition Key Generation

The partition key is generated using the following formula:
```
{logGroupId}-{schemaName}-{distributionFactor}-{retryAttempt}
```

This ensures:
- Even distribution across Kinesis shards
- Deterministic partitioning for the same input
- Load distribution based on the distribution factor

## Compression

All records are compressed using GZIP before being sent to Kinesis. This reduces:
- Network bandwidth usage
- Kinesis storage costs
- Transfer time

## Large Payload Handling

When a payload exceeds the threshold:
1. The payload is serialized as a `DataFileEntry`
2. It's stored in S3 using the `LargePayloadStore` interface
3. A `LargeDataFileEntry` with the S3 key is sent to Kinesis
4. The S3 key follows the format: `{logGroupId}-{schemaName}-{streamName}/{uuid}`

## Dependencies

- `github.com/aws/aws-sdk-go-v2`: AWS SDK v2 for Go
- `github.com/google/uuid`: UUID generation
- `github.com/prometheus/client_golang`: Prometheus metrics
- `github.com/gogo/protobuf`: Protobuf serialization

## Testing

The implementation can be tested using:
- Unit tests for individual components
- Integration tests with AWS services
- Mock implementations for the `LargePayloadStore` interface

## Performance Considerations

- **Batch Size**: Optimal batch sizes depend on your use case and Kinesis limits
- **Large Payload Threshold**: Set based on your Kinesis record size limits
- **Retry Configuration**: Balance between reliability and performance
- **Metrics**: Monitor metrics to identify bottlenecks and optimize performance

## Security

- Uses AWS SDK v2 for secure AWS service communication
- Supports AWS IAM roles and policies
- No sensitive data is logged or exposed in metrics
- S3 storage uses the same AWS credentials as Kinesis operations 