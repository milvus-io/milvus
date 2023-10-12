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

package datanode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

const (
	collectionID0   = UniqueID(2)
	collectionID1   = UniqueID(1)
	collectionName0 = "collection_0"
	collectionName1 = "collection_1"
)

func TestMetaService_All(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meta := NewMetaFactory().GetCollectionMeta(collectionID0, collectionName0, schemapb.DataType_Int64)
	broker := broker.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Status(nil),
			Schema: meta.GetSchema(),
		}, nil).Maybe()

	ms := newMetaService(broker, collectionID0)

	t.Run("Test getCollectionSchema", func(t *testing.T) {
		sch, err := ms.getCollectionSchema(ctx, collectionID0, 0)
		assert.NoError(t, err)
		assert.NotNil(t, sch)
		assert.Equal(t, sch.Name, collectionName0)
	})

	t.Run("Test printCollectionStruct", func(t *testing.T) {
		mf := &MetaFactory{}
		collectionMeta := mf.GetCollectionMeta(collectionID0, collectionName0, schemapb.DataType_Int64)
		printCollectionStruct(collectionMeta)
	})
}

// RootCoordFails1 root coord mock for failure
type RootCoordFails1 struct {
	RootCoordFactory
}

// DescribeCollectionInternal override method that will fails
func (rc *RootCoordFails1) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, errors.New("always fail")
}

// RootCoordFails2 root coord mock for failure
type RootCoordFails2 struct {
	RootCoordFactory
}

// DescribeCollectionInternal override method that will fails
func (rc *RootCoordFails2) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
	}, nil
}

func TestMetaServiceRootCoodFails(t *testing.T) {
	t.Run("Test Describe with error", func(t *testing.T) {
		rc := &RootCoordFails1{}
		rc.setCollectionID(collectionID0)
		rc.setCollectionName(collectionName0)

		broker := broker.NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		ms := newMetaService(broker, collectionID0)
		_, err := ms.getCollectionSchema(context.Background(), collectionID1, 0)
		assert.Error(t, err)
	})
}
