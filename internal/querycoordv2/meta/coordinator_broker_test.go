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

	"github.com/milvus-io/milvus-proto/go-api/schemapb"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
)

func TestCoordinatorBroker_GetCollectionSchema(t *testing.T) {
	t.Run("got error on DescribeCollectionInternal", func(t *testing.T) {
		rootCoord := mocks.NewRootCoord(t)
		rootCoord.On("DescribeCollectionInternal",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock DescribeCollectionInternal"))
		ctx := context.Background()
		broker := &CoordinatorBroker{rootCoord: rootCoord}
		_, err := broker.GetCollectionSchema(ctx, 100)
		assert.Error(t, err)
	})

	t.Run("non-success code", func(t *testing.T) {
		rootCoord := mocks.NewRootCoord(t)
		rootCoord.On("DescribeCollectionInternal",
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
		rootCoord.On("DescribeCollectionInternal",
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
