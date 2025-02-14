// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/common/errors"
	"github.com/milvus-io/milvus/internal/storagev2/common/log"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs/file"
)

type MinioFs struct {
	client     *minio.Client
	bucketName string
	path       string
}

func (fs *MinioFs) MkdirAll(dir string, i int) error {
	// TODO implement me
	panic("implement me")
}

func (fs *MinioFs) OpenFile(path string) (file.File, error) {
	err, bucket, path := getRealPath(path)
	if err != nil {
		return nil, err
	}
	return file.NewMinioFile(fs.client, path, bucket)
}

func (fs *MinioFs) Rename(src string, dst string) error {
	err, dstBucket, dst := getRealPath(dst)
	if err != nil {
		return err
	}
	err, srcBucket, src := getRealPath(src)
	if err != nil {
		return err
	}
	_, err = fs.client.CopyObject(context.TODO(), minio.CopyDestOptions{Bucket: dstBucket, Object: dst}, minio.CopySrcOptions{Bucket: srcBucket, Object: src})
	if err != nil {
		return err
	}
	err = fs.client.RemoveObject(context.TODO(), srcBucket, src, minio.RemoveObjectOptions{})
	if err != nil {
		log.Warn("failed to remove source object", log.String("source", src))
	}
	return nil
}

func (fs *MinioFs) DeleteFile(path string) error {
	err, bucket, path := getRealPath(path)
	if err != nil {
		return err
	}
	return fs.client.RemoveObject(context.TODO(), bucket, path, minio.RemoveObjectOptions{})
}

func (fs *MinioFs) CreateDir(path string) error {
	return nil
}

func (fs *MinioFs) List(prefix string) ([]FileEntry, error) {
	err, bucket, prefix := getRealPath(prefix)
	if err != nil {
		return nil, err
	}
	ret := make([]FileEntry, 0)
	for objInfo := range fs.client.ListObjects(context.TODO(), bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if objInfo.Err != nil {
			log.Warn("list object error", zap.Error(objInfo.Err))
			return nil, objInfo.Err
		}
		ret = append(ret, FileEntry{Path: path.Join(bucket, objInfo.Key)})
	}
	return ret, nil
}

func (fs *MinioFs) ReadFile(path string) ([]byte, error) {
	err, bucket, path := getRealPath(path)
	if err != nil {
		return nil, err
	}
	obj, err := fs.client.GetObject(context.TODO(), bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	stat, err := obj.Stat()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, stat.Size)
	n, err := obj.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != int(stat.Size) {
		return nil, fmt.Errorf("failed to read full file, expect: %d, actual: %d", stat.Size, n)
	}
	return buf, nil
}

func (fs *MinioFs) Exist(path string) (bool, error) {
	err, bucket, path := getRealPath(path)
	if err != nil {
		return false, err
	}
	_, err = fs.client.StatObject(context.TODO(), bucket, path, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (fs *MinioFs) Path() string {
	return path.Join(fs.bucketName, strings.TrimPrefix(fs.path, "/"))
}

// uri should be s3://username:password@bucket/path?endpoint_override=localhost%3A9000
func NewMinioFs(uri *url.URL) (*MinioFs, error) {
	accessKey := uri.User.Username()
	secretAccessKey, set := uri.User.Password()
	if !set {
		log.Warn("secret access key not set")
	}

	endpoints, ok := uri.Query()[constant.EndpointOverride]
	if !ok || len(endpoints) == 0 {
		return nil, errors.ErrNoEndpoint
	}

	cli, err := minio.New(endpoints[0], &minio.Options{
		BucketLookup: minio.BucketLookupAuto,
		Creds:        credentials.NewStaticV4(accessKey, secretAccessKey, ""),
	})
	if err != nil {
		return nil, err
	}

	bucket := uri.Host
	path := uri.Path

	log.Info("minio fs infos", zap.String("endpoint", endpoints[0]), zap.String("bucket", bucket), zap.String("path", path))

	exist, err := cli.BucketExists(context.TODO(), bucket)
	if err != nil {
		return nil, err
	}

	if !exist {
		if err = cli.MakeBucket(context.TODO(), bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}

	return &MinioFs{
		client:     cli,
		bucketName: bucket,
		path:       path,
	}, nil
}

func getRealPath(path string) (error, string, string) {
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("Invalid path, %s should not start with '/'", path), "", ""
	}
	words := strings.SplitN(path, "/", 2)
	if (len(words)) != 2 {
		return fmt.Errorf("Invalid path, %s should contains at least one '/'", path), "", ""
	}
	return nil, words[0], words[1]
}
