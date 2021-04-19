package miniokv

import (
	"context"
	"fmt"

	"io"
	"log"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type MinIOKV struct {
	ctx         context.Context
	minioClient *minio.Client
	bucketName  string
}

type Option struct {
	Address           string
	AccessKeyID       string
	BucketName        string
	SecretAccessKeyID string
	UseSSL            bool
	CreateBucket      bool // when bucket not existed, create it
}

func NewMinIOKV(ctx context.Context, option *Option) (*MinIOKV, error) {
	minIOClient, err := minio.New(option.Address, &minio.Options{
		Creds:  credentials.NewStaticV4(option.AccessKeyID, option.SecretAccessKeyID, ""),
		Secure: option.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	bucketExists, err := minIOClient.BucketExists(ctx, option.BucketName)
	if err != nil {
		return nil, err
	}

	if option.CreateBucket {
		if !bucketExists {
			err = minIOClient.MakeBucket(ctx, option.BucketName, minio.MakeBucketOptions{})
			if err != nil {
				return nil, err
			}
		}
	} else {
		if !bucketExists {
			return nil, errors.New(fmt.Sprintf("Bucket %s not Existed.", option.BucketName))
		}
	}

	return &MinIOKV{
		ctx:         ctx,
		minioClient: minIOClient,
		bucketName:  option.BucketName,
	}, nil
}

func (kv *MinIOKV) LoadWithPrefix(key string) ([]string, []string, error) {
	objects := kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: key})

	var objectsKeys []string
	var objectsValues []string

	for object := range objects {
		objectsKeys = append(objectsKeys, object.Key)
	}
	objectsValues, err := kv.MultiLoad(objectsKeys)
	if err != nil {
		log.Printf("cannot load value with prefix:%s", key)
	}

	return objectsKeys, objectsValues, nil
}

func (kv *MinIOKV) Load(key string) (string, error) {
	object, err := kv.minioClient.GetObject(kv.ctx, kv.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}

	buf := new(strings.Builder)
	_, err = io.Copy(buf, object)
	if err != nil && err != io.EOF {
		return "", err
	}
	return buf.String(), nil
}

func (kv *MinIOKV) MultiLoad(keys []string) ([]string, error) {
	var resultErr error
	var objectsValues []string
	for _, key := range keys {
		objectValue, err := kv.Load(key)
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, resultErr
}

func (kv *MinIOKV) Save(key, value string) error {
	reader := strings.NewReader(value)
	_, err := kv.minioClient.PutObject(kv.ctx, kv.bucketName, key, reader, int64(len(value)), minio.PutObjectOptions{})

	if err != nil {
		return err
	}

	return err
}

func (kv *MinIOKV) MultiSave(kvs map[string]string) error {
	var resultErr error
	for key, value := range kvs {
		err := kv.Save(key, value)
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
		}
	}
	return resultErr
}

func (kv *MinIOKV) RemoveWithPrefix(prefix string) error {
	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)

		for object := range kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: prefix}) {
			objectsCh <- object
		}
	}()

	for rErr := range kv.minioClient.RemoveObjects(kv.ctx, kv.bucketName, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
		if rErr.Err != nil {
			return rErr.Err
		}
	}
	return nil
}

func (kv *MinIOKV) Remove(key string) error {
	err := kv.minioClient.RemoveObject(kv.ctx, kv.bucketName, string(key), minio.RemoveObjectOptions{})
	return err
}

func (kv *MinIOKV) MultiRemove(keys []string) error {
	var resultErr error
	for _, key := range keys {
		err := kv.Remove(key)
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
		}
	}
	return resultErr
}

func (kv *MinIOKV) Close() {

}
