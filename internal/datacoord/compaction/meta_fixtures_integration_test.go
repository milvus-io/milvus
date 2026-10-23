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

package compaction

import (
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
)

func newMock0Allocator(t *testing.T) *allocator.MockAllocator {
	mock0Allocator := allocator.NewMockAllocator(t)
	mock0Allocator.EXPECT().AllocID(mock.Anything).Return(100, nil).Maybe()
	mock0Allocator.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil).Maybe()
	mock0Allocator.EXPECT().AllocN(mock.Anything).RunAndReturn(func(i int64) (int64, int64, error) {
		return 100, 100 + i, nil
	}).Maybe()
	return mock0Allocator
}

func newTestScalarClusteringKeySchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test_scalar_clustering",
		Description: "schema for test scalar clustering compaction",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "field1", IsPrimaryKey: true, IsClusteringKey: true, Description: "field no.1", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}
