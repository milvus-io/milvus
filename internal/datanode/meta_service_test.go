// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
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

	mFactory := &RootCoordFactory{}
	mFactory.setCollectionID(collectionID0)
	mFactory.setCollectionName(collectionName0)
	ms := newMetaService(mFactory, collectionID0)

	t.Run("Test getCollectionSchema", func(t *testing.T) {

		sch, err := ms.getCollectionSchema(ctx, collectionID0, 0)
		assert.NoError(t, err)
		assert.NotNil(t, sch)
		assert.Equal(t, sch.Name, collectionName0)
	})

	t.Run("Test printCollectionStruct", func(t *testing.T) {
		mf := &MetaFactory{}
		collectionMeta := mf.CollectionMetaFactory(collectionID0, collectionName0)
		printCollectionStruct(collectionMeta)
	})
}

//RootCoordFails1 root coord mock for failure
type RootCoordFails1 struct {
	RootCoordFactory
}

// DescribeCollection override method that will fails
func (rc *RootCoordFails1) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, errors.New("always fail")
}

//RootCoordFails2 root coord mock for failure
type RootCoordFails2 struct {
	RootCoordFactory
}

// DescribeCollection override method that will fails
func (rc *RootCoordFails2) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
	}, nil
}

func TestMetaServiceRootCoodFails(t *testing.T) {

	t.Run("Test Describe with error", func(t *testing.T) {
		rc := &RootCoordFails1{}
		rc.setCollectionID(collectionID0)
		rc.setCollectionName(collectionName0)

		ms := newMetaService(rc, collectionID0)
		_, err := ms.getCollectionSchema(context.Background(), collectionID1, 0)
		assert.NotNil(t, err)
	})

	t.Run("Test Describe wit nil response", func(t *testing.T) {
		rc := &RootCoordFails2{}
		rc.setCollectionID(collectionID0)
		rc.setCollectionName(collectionName0)

		ms := newMetaService(rc, collectionID0)
		_, err := ms.getCollectionSchema(context.Background(), collectionID1, 0)
		assert.NotNil(t, err)
	})
}
