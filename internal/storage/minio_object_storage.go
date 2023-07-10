package storage

import (
	"bytes"
	"context"
	"github.com/milvus-io/milvus/internal/storage/aliyun"
	"github.com/milvus-io/milvus/internal/storage/gcp"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type minioObjectStorage struct {
	Client     *minio.Client
	bucketName string
	rootPath   string
}

func (m *minioObjectStorage) Stat(ctx context.Context, name string) (*fileInfo, error) {
	object, err := m.Client.StatObject(ctx, m.bucketName, name, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	return &fileInfo{
		Size:         object.Size,
		LastModified: object.LastModified,
		Key:          object.Key,
		Err:          object.Err,
	}, err
}

type minioObjIter struct {
	ch  <-chan minio.ObjectInfo
	obj []minio.ObjectInfo
}

func (m *minioObjIter) HasNext(ctx context.Context) bool {
	if len(m.obj) != 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case t, ok := <-m.ch:
		if !ok {
			return false // close
		}
		m.obj = append(m.obj, t)
		return true
	}
}

func (m *minioObjIter) Next(context.Context) fileInfo {
	ans := m.obj[0]
	m.obj = m.obj[:0]
	return fileInfo{
		Size:         ans.Size,
		LastModified: ans.LastModified,
		Key:          ans.Key,
		Err:          ans.Err,
	}
}

func (m *minioObjectStorage) ListWithPrefix(ctx context.Context, preFix string, recursive bool) (iterator, error) {
	objects := m.Client.ListObjects(ctx, m.bucketName, minio.ListObjectsOptions{Prefix: preFix, Recursive: recursive})
	return &minioObjIter{
		ch:  objects,
		obj: nil,
	}, nil
}

func (m *minioObjectStorage) CreateBucket(ctx context.Context) error {
	err := m.Client.MakeBucket(ctx, m.bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return nil
	}
	return nil
}

func (m *minioObjectStorage) Get(ctx context.Context, name string, off, length int64) (FileReader, error) {
	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, length)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	return m.Client.GetObject(ctx, m.bucketName, name, opts)
}

func (m *minioObjectStorage) Write(ctx context.Context, name string, data []byte) error {
	_, err := m.Client.PutObject(ctx, m.bucketName, name, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	return err
}

func (m *minioObjectStorage) Delete(ctx context.Context, name string) error {
	return m.Client.RemoveObject(ctx, m.bucketName, name, minio.RemoveObjectOptions{})
}

func newMinioObjectStorage(c *config) (ObjectStorage, error) {
	var creds *credentials.Credentials
	var newMinioFn = minio.New
	var bucketLookupType = minio.BucketLookupAuto

	switch c.cloudProvider {
	case CloudProviderAliyun:
		// auto doesn't work for aliyun, so we set to dns deliberately
		bucketLookupType = minio.BucketLookupDNS
		if c.useIAM {
			newMinioFn = aliyun.NewMinioClient
		} else {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}
	case CloudProviderGCP:
		newMinioFn = gcp.NewMinioClient
		if !c.useIAM {
			creds = credentials.NewStaticV2(c.accessKeyID, c.secretAccessKeyID, "")
		}
	default: // aws, minio
		if c.useIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}
	}
	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       c.useSSL,
	}
	minIOClient, err := newMinioFn(c.address, minioOpts)
	if err != nil {
		return nil, err
	}
	return &minioObjectStorage{
		Client:     minIOClient,
		bucketName: c.bucketName,
		rootPath:   c.rootPath,
	}, nil
}
