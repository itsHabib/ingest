package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Store implements ObjectStore using S3-compatible storage (AWS S3 or MinIO).
// S3Store is safe for concurrent use because the underlying s3.Client is thread-safe.
type S3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store creates an S3Store pointing at the given endpoint and bucket.
// It ensures the bucket exists, creating it if necessary.
func NewS3Store(endpoint, bucket, accessKey, secretKey, region string, usePathStyle bool) (*S3Store, error) {
	client := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		UsePathStyle: usePathStyle,
	})

	store := &S3Store{
		client: client,
		bucket: bucket,
	}

	// Ensure the bucket exists.
	if err := store.ensureBucket(context.Background()); err != nil {
		return nil, fmt.Errorf("s3: ensure bucket: %w", err)
	}

	return store, nil
}

// ensureBucket creates the bucket if it does not already exist.
func (s *S3Store) ensureBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err == nil {
		return nil
	}

	// Bucket does not exist, create it.
	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return fmt.Errorf("s3: create bucket: %w", err)
	}
	return nil
}

// Put writes data to the given key with Content-Type application/json.
func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("s3: put %q: %w", key, err)
	}
	return nil
}

// Get reads data from the given key.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: get %q read body: %w", key, err)
	}
	return data, nil
}

// List returns all keys matching the given prefix, paginating through all results.
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string

	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list %q: %w", prefix, err)
		}

		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return keys, nil
}

// Delete removes the key. Returns nil if the key does not exist (idempotent).
func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3: delete %q: %w", key, err)
	}
	return nil
}

// Exists returns true if the key exists.
func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) {
			return false, nil
		}
		// Also check for NoSuchKey via the error message, as some S3-compatible
		// implementations return different error types.
		var nsk2 *types.NoSuchKey
		if errors.As(err, &nsk2) {
			return false, nil
		}
		return false, fmt.Errorf("s3: exists %q: %w", key, err)
	}
	return true, nil
}
