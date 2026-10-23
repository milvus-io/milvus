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

	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewObjectStore exposes existing server storage through the public object API.
// Remote calls retain the provider client, retries, metrics and copy policy.
func NewObjectStore(cm ChunkManager) objectstore.ObjectStore { return &chunkObjectStore{cm: cm} }

type chunkObjectStore struct{ cm ChunkManager }

func (s *chunkObjectStore) forObject(object objectstore.Object) ChunkManager {
	if remote, ok := s.cm.(*RemoteChunkManager); ok {
		scoped := *remote
		scoped.bucketName = object.Bucket
		return &scoped
	}
	return s.cm
}

func (s *chunkObjectStore) Read(ctx context.Context, object objectstore.Object) ([]byte, error) {
	return s.forObject(object).Read(ctx, object.Key)
}

func (s *chunkObjectStore) Write(ctx context.Context, object objectstore.Object, data []byte) error {
	return s.forObject(object).Write(ctx, object.Key, data)
}

func (s *chunkObjectStore) Copy(ctx context.Context, req objectstore.CopyRequest) (int64, error) {
	if s.cm == nil {
		return 0, merr.WrapErrServiceInternalMsg("chunk manager is nil")
	}
	var size int64
	var err error
	if remote, ok := s.cm.(*RemoteChunkManager); ok {
		size, err = remote.copyCrossBucket(ctx, req.Source.Bucket, req.Source.Key, req.Destination.Bucket, req.Destination.Key)
	} else {
		if req.Source.Bucket != req.Destination.Bucket {
			return 0, merr.Wrap(merr.ErrServiceUnimplemented, "local storage cannot copy across buckets")
		}
		err = s.cm.Copy(ctx, req.Source.Key, req.Destination.Key)
		if err == nil {
			size, err = s.cm.Size(ctx, req.Destination.Key)
			if err == nil && size < 0 {
				err = merr.WrapErrDataIntegrityMsg("copied object size cannot be negative")
			}
			if err != nil {
				err = &objectstore.CopySizeError{Err: err}
			}
		}
	}
	if err != nil {
		return 0, err
	}
	return size, nil
}
