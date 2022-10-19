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

package proxy

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
)

func TestGetIndexStateTask_Execute(t *testing.T) {
	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	fieldName := funcutil.GenRandomStr()
	indexName := ""
	ctx := context.Background()

	rootCoord := newMockRootCoord()
	indexCoord := newMockIndexCoord()
	queryCoord := NewQueryCoordMock()

	gist := &getIndexStateTask{
		GetIndexStateRequest: &milvuspb.GetIndexStateRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      fieldName,
			IndexName:      indexName,
		},
		ctx:        ctx,
		indexCoord: indexCoord,
		rootCoord:  rootCoord,
		result: &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
			State:  commonpb.IndexState_Unissued,
		},
		collectionID: collectionID,
	}

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.Error(t, gist.Execute(ctx))

	rootCoord.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	indexCoord.GetIndexStateFunc = func(ctx context.Context, request *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
		return &indexpb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			State:      commonpb.IndexState_Finished,
			FailReason: "",
		}, nil
	}

	assert.NoError(t, gist.Execute(ctx))
	assert.Equal(t, commonpb.IndexState_Finished, gist.result.GetState())
}

func TestDropIndexTask_PreExecute(t *testing.T) {
	collectionName := "collection1"
	collectionID := UniqueID(1)
	fieldName := "field1"
	indexName := ""

	Params.Init()
	rc := newMockRootCoord()
	showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionIDs: nil,
		}, nil
	}
	qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
	ic := newMockIndexCoord()
	ctx := context.Background()
	qc.updateState(commonpb.StateCode_Healthy)

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, rc, qc, shardMgr)

	rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	dit := dropIndexTask{
		ctx: ctx,
		DropIndexRequest: &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
				TargetID:  0,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
			IndexName:      indexName,
		},
		indexCoord:   ic,
		queryCoord:   qc,
		result:       nil,
		collectionID: collectionID,
	}

	t.Run("normal", func(t *testing.T) {
		err := dit.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("coll has been loaded", func(t *testing.T) {
		showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				CollectionIDs: []int64{collectionID},
			}, nil
		}
		qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
		qc.updateState(commonpb.StateCode_Healthy)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection error", func(t *testing.T) {
		showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
			return nil, errors.New("error")
		}
		qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
		qc.updateState(commonpb.StateCode_Healthy)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection fail", func(t *testing.T) {
		showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "fail reason",
				},
			}, nil
		}
		qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
		qc.updateState(commonpb.StateCode_Healthy)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})
}
