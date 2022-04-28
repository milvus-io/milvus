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
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func TestGetIndexStateTask_Execute(t *testing.T) {
	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	fieldName := funcutil.GenRandomStr()
	indexName := ""
	ctx := context.Background()
	partitions := []UniqueID{2}
	indexID := UniqueID(3)
	segmentID := UniqueID(4)
	indexBuildID := UniqueID(5)

	rootCoord := newMockRootCoord()
	indexCoord := newMockIndexCoord()

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

	// failed to get collection id.
	_ = InitMetaCache(rootCoord)
	assert.Error(t, gist.Execute(ctx))
	rootCoord.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Schema:       &schemapb.CollectionSchema{},
			CollectionID: collectionID,
		}, nil
	}

	// failed to show partitions.
	assert.Error(t, gist.Execute(ctx))
	rootCoord.ShowPartitionsFunc = func(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
		return &milvuspb.ShowPartitionsResponse{
			Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
			PartitionIDs: partitions,
		}, nil
	}
	assert.Error(t, gist.Execute(ctx))
	rootCoord.ShowPartitionsFunc = func(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
		return &milvuspb.ShowPartitionsResponse{
			Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			PartitionIDs: partitions,
		}, nil
	}

	// failed to describe index.
	assert.Error(t, gist.Execute(ctx))
	rootCoord.DescribeIndexFunc = func(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
		return &milvuspb.DescribeIndexResponse{
			Status:            &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
			IndexDescriptions: nil,
		}, nil
	}
	assert.Error(t, gist.Execute(ctx))

	// no index is created.
	rootCoord.DescribeIndexFunc = func(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
		return &milvuspb.DescribeIndexResponse{
			Status:            &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			IndexDescriptions: nil,
		}, nil
	}
	assert.Error(t, gist.Execute(ctx))
	rootCoord.DescribeIndexFunc = func(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			IndexDescriptions: []*milvuspb.IndexDescription{
				{
					IndexName: Params.CommonCfg.DefaultIndexName,
					IndexID:   indexID,
					Params:    nil,
					FieldName: fieldName,
				},
			},
		}, nil
	}

	// failed to get segment ids.
	assert.Error(t, gist.Execute(ctx))
	rootCoord.ShowSegmentsFunc = func(ctx context.Context, request *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
		return &milvuspb.ShowSegmentsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
		}, nil
	}
	assert.Error(t, gist.Execute(ctx))
	rootCoord.ShowSegmentsFunc = func(ctx context.Context, request *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
		return &milvuspb.ShowSegmentsResponse{
			Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			SegmentIDs: []UniqueID{segmentID},
		}, nil
	}

	// failed to get index build ids.
	assert.Error(t, gist.Execute(ctx))
	rootCoord.DescribeSegmentsFunc = func(ctx context.Context, request *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
		return &rootcoordpb.DescribeSegmentsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
		}, nil
	}
	assert.Error(t, gist.Execute(ctx))
	rootCoord.DescribeSegmentsFunc = func(ctx context.Context, request *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
		return &rootcoordpb.DescribeSegmentsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			SegmentInfos: map[int64]*rootcoordpb.SegmentInfos{
				segmentID: {
					IndexInfos: []*etcdpb.SegmentIndexInfo{
						{
							IndexID:     indexID,
							EnableIndex: true,
							BuildID:     indexBuildID,
						},
					},
				},
			},
		}, nil
	}

	// failed to get index states.
	assert.Error(t, gist.Execute(ctx))
	indexCoord.GetIndexStatesFunc = func(ctx context.Context, request *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
		return &indexpb.GetIndexStatesResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
		}, nil
	}

	// normal case.
	assert.NoError(t, gist.Execute(ctx))
	assert.Equal(t, commonpb.IndexState_Failed, gist.result.GetState())
	indexCoord.GetIndexStatesFunc = func(ctx context.Context, request *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
		return &indexpb.GetIndexStatesResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			States: []*indexpb.IndexInfo{
				{
					State: commonpb.IndexState_Unissued,
				},
			},
		}, nil
	}
	assert.NoError(t, gist.Execute(ctx))
	assert.Equal(t, commonpb.IndexState_Unissued, gist.result.GetState())
	indexCoord.GetIndexStatesFunc = func(ctx context.Context, request *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
		return &indexpb.GetIndexStatesResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			States: []*indexpb.IndexInfo{
				{
					State: commonpb.IndexState_Finished,
				},
			},
		}, nil
	}
	assert.NoError(t, gist.Execute(ctx))
	assert.Equal(t, commonpb.IndexState_Finished, gist.result.GetState())
}
