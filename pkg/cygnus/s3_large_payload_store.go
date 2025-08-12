package cygnus

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3LargePayloadStore implements LargePayloadStore interface using S3
type S3LargePayloadStore struct {
	s3Client *s3.Client
	bucket   string
}

// NewS3LargePayloadStore creates a new S3 large payload store
func NewS3LargePayloadStore(s3Client *s3.Client, bucket string) *S3LargePayloadStore {
	return &S3LargePayloadStore{
		s3Client: s3Client,
		bucket:   bucket,
	}
}

// Write stores data in S3 with the given key
func (s *S3LargePayloadStore) Write(ctx context.Context, key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	_, err := s.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to write to S3: %w", err)
	}

	return nil
} 