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

package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestUpdateExternalSchemaViaWAL_CollectionNotFound(t *testing.T) {
	ctx := context.Background()

	mockGetCloned := mockey.Mock((*meta).GetClonedCollectionInfo).Return(nil).Build()
	defer mockGetCloned.UnPatch()

	server := &Server{
		meta: &meta{},
	}

	err := server.updateExternalSchemaViaWAL(ctx, 999, "s3://bucket/new", `{"format":"parquet"}`)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrCollectionNotFound))
}

func TestUpdateExternalSchemaViaWAL_AlterCollectionRPCError(t *testing.T) {
	ctx := context.Background()

	mockGetCloned := mockey.Mock((*meta).GetClonedCollectionInfo).Return(&collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://bucket/old",
			ExternalSpec:   `{"format":"parquet"}`,
		},
		DatabaseName: "default",
	}).Build()
	defer mockGetCloned.UnPatch()

	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().AlterCollection(mock.Anything, mock.Anything).
		Return(nil, errors.New("rpc connection error"))

	server := &Server{
		meta:     &meta{},
		mixCoord: mixCoord,
	}

	err := server.updateExternalSchemaViaWAL(ctx, 100, "s3://bucket/new", `{"format":"parquet","version":2}`)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rpc connection error")
}

func TestUpdateExternalSchemaViaWAL_AlterCollectionStatusError(t *testing.T) {
	ctx := context.Background()

	mockGetCloned := mockey.Mock((*meta).GetClonedCollectionInfo).Return(&collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://bucket/old",
			ExternalSpec:   `{"format":"parquet"}`,
		},
		DatabaseName: "default",
	}).Build()
	defer mockGetCloned.UnPatch()

	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().AlterCollection(mock.Anything, mock.Anything).
		Return(merr.Status(merr.WrapErrCollectionNotFound(100)), nil)

	server := &Server{
		meta:     &meta{},
		mixCoord: mixCoord,
	}

	err := server.updateExternalSchemaViaWAL(ctx, 100, "s3://bucket/new", `{"format":"parquet","version":2}`)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrCollectionNotFound))
}

func TestUpdateExternalSchemaViaWAL_Success(t *testing.T) {
	ctx := context.Background()

	mockGetCloned := mockey.Mock((*meta).GetClonedCollectionInfo).Return(&collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://bucket/old",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		},
		DatabaseName:  "default",
		DatabaseID:    1,
		VChannelNames: []string{"ch-0", "ch-1"},
	}).Build()
	defer mockGetCloned.UnPatch()

	var capturedReq *milvuspb.AlterCollectionRequest
	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().AlterCollection(mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *milvuspb.AlterCollectionRequest) {
			capturedReq = req
		}).
		Return(merr.Success(), nil)

	server := &Server{
		meta:     &meta{},
		mixCoord: mixCoord,
	}

	newSource := "s3://bucket/new-path"
	newSpec := `{"format":"parquet","version":2}`
	err := server.updateExternalSchemaViaWAL(ctx, 100, newSource, newSpec)
	assert.NoError(t, err)

	// Verify request was sent with correct parameters
	assert.NotNil(t, capturedReq)
	assert.Equal(t, "default", capturedReq.GetDbName())
	assert.Equal(t, "test_collection", capturedReq.GetCollectionName())
	assert.Len(t, capturedReq.GetProperties(), 2)

	propMap := make(map[string]string)
	for _, kv := range capturedReq.GetProperties() {
		propMap[kv.GetKey()] = kv.GetValue()
	}
	assert.Equal(t, newSource, propMap[common.CollectionExternalSource])
	assert.Equal(t, newSpec, propMap[common.CollectionExternalSpec])
}
