package s3driver

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	. "github.com/zilliztech/milvus-distributed/internal/storage/type"
)

var bucketName = conf.Config.Writer.Bucket

type S3Store struct {
	client *s3.S3
}

func NewS3Store(config aws.Config) (*S3Store, error) {
	sess := session.Must(session.NewSession(&config))
	service := s3.New(sess)
	return &S3Store{
		client: service,
	}, nil
}

func (s *S3Store) Put(ctx context.Context, key Key, value Value) error {
	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(string(key)),
		Body:   bytes.NewReader(value),
	})

	//sess := session.Must(session.NewSessionWithOptions(session.Options{
	//	SharedConfigState: session.SharedConfigEnable,
	//}))
	//uploader := s3manager.NewUploader(sess)
	//
	//_, err := uploader.Upload(&s3manager.UploadInput{
	//	Bucket: aws.String(bucketName),
	//	Key:    aws.String(string(key)),
	//	Body:   bytes.NewReader(value),
	//})

	return err
}

func (s *S3Store) Get(ctx context.Context, key Key) (Value, error) {
	object, err := s.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(string(key)),
	})
	if err != nil {
		return nil, err
	}

	//TODO: get size
	size := 256 * 1024
	buf := make([]byte, size)
	n, err := object.Body.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

func (s *S3Store) GetByPrefix(ctx context.Context, prefix Key, keyOnly bool) ([]Key, []Value, error) {
	objectsOutput, err := s.client.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(string(prefix)),
	})

	var objectsKeys []Key
	var objectsValues []Value

	if objectsOutput != nil && err == nil {
		for _, object := range objectsOutput.Contents {
			objectsKeys = append(objectsKeys, []byte(*object.Key))
			if !keyOnly {
				value, err := s.Get(ctx, []byte(*object.Key))
				if err != nil {
					return nil, nil, err
				}
				objectsValues = append(objectsValues, value)
			}
		}
	} else {
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil

}

func (s *S3Store) Scan(ctx context.Context, keyStart Key, keyEnd Key, limit int, keyOnly bool) ([]Key, []Value, error) {
	var keys []Key
	var values []Value
	limitCount := uint(limit)
	objects, err := s.client.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(string(keyStart)),
	})
	if err == nil && objects != nil {
		for _, object := range objects.Contents {
			if *object.Key >= string(keyEnd) {
				keys = append(keys, []byte(*object.Key))
				if !keyOnly {
					value, err := s.Get(ctx, []byte(*object.Key))
					if err != nil {
						return nil, nil, err
					}
					values = append(values, value)
				}
				limitCount--
				if limitCount <= 0 {
					break
				}
			}
		}
	}

	return keys, values, err
}

func (s *S3Store) Delete(ctx context.Context, key Key) error {
	_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(string(key)),
	})
	return err
}

func (s *S3Store) DeleteByPrefix(ctx context.Context, prefix Key) error {

	objects, err := s.client.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(string(prefix)),
	})

	if objects != nil && err == nil {
		for _, object := range objects.Contents {
			_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    object.Key,
			})
			return err
		}
	}

	return nil
}

func (s *S3Store) DeleteRange(ctx context.Context, keyStart Key, keyEnd Key) error {

	objects, err := s.client.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(string(keyStart)),
	})

	if objects != nil && err == nil {
		for _, object := range objects.Contents {
			if *object.Key > string(keyEnd) {
				_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    object.Key,
				})
				return err
			}
		}
	}

	return nil
}
