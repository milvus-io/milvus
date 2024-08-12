// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

var _ ObjectStorage = (*GCPNativeObjectStorage)(nil)

type GCPNativeObjectStorage struct {
	client *storage.Client
}

func newGCPNativeObjectStorageWithConfig(ctx context.Context, c *config) (*GCPNativeObjectStorage, error) {
	var client *storage.Client
	var err error

	var opts []option.ClientOption
	var projectId string
	if c.address != "" {
		opts = append(opts, option.WithEndpoint(c.address))
	}
	if c.gcpNativeWithoutAuth {
		opts = append(opts, option.WithoutAuthentication())
	} else {
		creds, err := google.CredentialsFromJSON(ctx, []byte(c.gcpCredentialJSON), storage.ScopeReadWrite)
		if err != nil {
			return nil, err
		}
		projectId, err = getProjectId(c.gcpCredentialJSON)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentials(creds))
	}

	client, err = storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if c.bucketName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("invalid empty bucket name")
	}
	// Check bucket validity
	checkBucketFn := func() error {
		bucket := client.Bucket(c.bucketName)
		_, err := bucket.Attrs(ctx)
		if err == storage.ErrBucketNotExist && c.createBucket {
			err = client.Bucket(c.bucketName).Create(ctx, projectId, nil)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return &GCPNativeObjectStorage{client: client}, nil
}

func (gcs *GCPNativeObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	bucket := gcs.client.Bucket(bucketName)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}

	obj := bucket.Object(objectName)
	_, err = obj.Attrs(ctx)
	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}
	var reader *storage.Reader
	if offset == 0 && size == 0 {
		reader, err = obj.NewReader(ctx)
	} else {
		reader, err = obj.NewRangeReader(ctx, offset, size)
	}

	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}
	return &GCSReader{reader: reader, obj: obj}, nil
}

func (gcs *GCPNativeObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	obj := gcs.client.Bucket(bucketName).Object(objectName)
	writer := obj.NewWriter(ctx)
	_, err := io.Copy(writer, reader)
	if err != nil {
		return checkObjectStorageError(objectName, err)
	}
	err = writer.Close()
	if err != nil {
		return checkObjectStorageError(objectName, err)
	}
	return nil
}

func (gcs *GCPNativeObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	obj := gcs.client.Bucket(bucketName).Object(objectName)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return 0, checkObjectStorageError(objectName, err)
	}
	return attrs.Size, nil
}

func (GCSObjectStorage *GCPNativeObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	query := &storage.Query{
		Prefix: prefix,
	}
	if !recursive {
		query.Delimiter = "/"
	}

	it := GCSObjectStorage.client.Bucket(bucketName).Objects(ctx, query)
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if objAttrs.Prefix != "" {
			continue
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: objAttrs.Name, ModifyTime: objAttrs.Updated}) {
			return nil
		}
	}
	return nil
}

func (gcs *GCPNativeObjectStorage) RemoveObject(ctx context.Context, bucketName, prefix string) error {
	bucket := gcs.client.Bucket(bucketName)
	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return checkObjectStorageError(prefix, err)
		}

		obj := bucket.Object(objAttrs.Name)
		if err := obj.Delete(ctx); err != nil {
			return checkObjectStorageError(objAttrs.Name, err)
		}
	}

	return nil
}

func (gcs *GCPNativeObjectStorage) DeleteBucket(ctx context.Context, bucketName string) error {
	bucket := gcs.client.Bucket(bucketName)

	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return checkObjectStorageError(objAttrs.Name, err)
		}

		err = gcs.RemoveObject(ctx, bucketName, objAttrs.Name)
		if err != nil {
			return checkObjectStorageError(objAttrs.Name, err)
		}
	}

	err := bucket.Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

type GCSReader struct {
	reader   *storage.Reader
	obj      *storage.ObjectHandle
	position int64
}

func (r *GCSReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if err != nil {
		return n, err
	}
	r.position = r.position + int64(n)
	return n, nil
}

func (r *GCSReader) Close() error {
	return r.reader.Close()
}

func (gr *GCSReader) ReadAt(p []byte, off int64) (n int, err error) {
	reader, err := gr.obj.NewRangeReader(context.Background(), off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.ReadFull(reader, p)
}

func (gr *GCSReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = gr.position + offset
	case io.SeekEnd:
		objectAttrs, err := gr.obj.Attrs(context.Background())
		if err != nil {
			return 0, err
		}
		newOffset = objectAttrs.Size + offset
	default:
		return 0, merr.WrapErrIoFailedReason("invalid whence")
	}

	if newOffset < 0 {
		return 0, merr.WrapErrIoFailedReason("negative offset")
	}

	// Reset the underlying reader to the new offset
	newReader, err := gr.obj.NewRangeReader(context.Background(), newOffset, -1)
	if err != nil {
		if gErr, ok := err.(*googleapi.Error); ok {
			if gErr.Code == 416 {
				newReader, _ = gr.obj.NewRangeReader(context.Background(), 0, 0)
			}
		} else {
			return 0, err
		}
	}

	// Update the reader and the current position
	gr.reader = newReader
	gr.position = newOffset
	return newOffset, nil
}

func getProjectId(gcpCredentialJSON string) (string, error) {
	if gcpCredentialJSON == "" {
		return "", errors.New("the JSON string is empty")
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(gcpCredentialJSON), &data); err != nil {
		return "", errors.New("failed to parse GOOGLE_APPLICATION_CREDENTIALS file")
	}
	propertyValue, ok := data["project_id"]
	projectId := fmt.Sprintf("%v", propertyValue)
	if !ok {
		return "", errors.New("projectId doesn't exist")
	}
	return projectId, nil
}
