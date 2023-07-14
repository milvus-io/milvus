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

package meta

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestCoordinatorBroker_GetCollectionSchema(t *testing.T) {
	t.Run("got error on DescribeCollection", func(t *testing.T) {
		rootCoord := mocks.NewRootCoord(t)
		rootCoord.On("DescribeCollection",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock DescribeCollection"))
		ctx := context.Background()
		broker := &CoordinatorBroker{rootCoord: rootCoord}
		_, err := broker.GetCollectionSchema(ctx, 100)
		assert.Error(t, err)
	})

	t.Run("non-success code", func(t *testing.T) {
		rootCoord := mocks.NewRootCoord(t)
		rootCoord.On("DescribeCollection",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists},
		}, nil)
		ctx := context.Background()
		broker := &CoordinatorBroker{rootCoord: rootCoord}
		_, err := broker.GetCollectionSchema(ctx, 100)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		rootCoord := mocks.NewRootCoord(t)
		rootCoord.On("DescribeCollection",
			mock.Anything,
			mock.Anything,
		).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Schema: &schemapb.CollectionSchema{Name: "test_schema"},
		}, nil)
		ctx := context.Background()
		broker := &CoordinatorBroker{rootCoord: rootCoord}
		schema, err := broker.GetCollectionSchema(ctx, 100)
		assert.NoError(t, err)
		assert.Equal(t, "test_schema", schema.GetName())
	})
}

func TestCoordinatorBroker_GetRecoveryInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		dc := mocks.NewMockDataCoord(t)
		dc.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{}, nil)

		ctx := context.Background()
		broker := &CoordinatorBroker{dataCoord: dc}

		_, _, err := broker.GetRecoveryInfoV2(ctx, 1)
		assert.NoError(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		dc := mocks.NewMockDataCoord(t)
		fakeErr := errors.New("fake error")
		dc.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(nil, fakeErr)

		ctx := context.Background()
		broker := &CoordinatorBroker{dataCoord: dc}

		_, _, err := broker.GetRecoveryInfoV2(ctx, 1)
		assert.ErrorIs(t, err, fakeErr)
	})

	t.Run("return non-success code", func(t *testing.T) {
		dc := mocks.NewMockDataCoord(t)
		dc.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil)

		ctx := context.Background()
		broker := &CoordinatorBroker{dataCoord: dc}

		_, _, err := broker.GetRecoveryInfoV2(ctx, 1)
		assert.Error(t, err)
	})
}

func TestCoordinatorBroker_GetPartitions(t *testing.T) {
	collection := int64(100)
	partitions := []int64{10, 11, 12}

	t.Run("normal case", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status:       &commonpb.Status{},
			PartitionIDs: partitions,
		}, nil)

		ctx := context.Background()
		broker := &CoordinatorBroker{rootCoord: rc}

		retPartitions, err := broker.GetPartitions(ctx, collection)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partitions, retPartitions)
	})

	t.Run("collection not exist", func(t *testing.T) {
		rc := mocks.NewRootCoord(t)
		rc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			},
		}, nil)

		ctx := context.Background()
		broker := &CoordinatorBroker{rootCoord: rc}
		_, err := broker.GetPartitions(ctx, collection)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}
