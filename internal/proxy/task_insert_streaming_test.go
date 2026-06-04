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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// buildMinimalInsertMsg creates a minimal InsertMsg with a single int64 PK row
// for use in unit tests.
func buildMinimalInsertMsg(collID int64, db, coll, partition string) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				SourceID: 1,
			},
			CollectionID:   collID,
			DbName:         db,
			CollectionName: coll,
			PartitionName:  partition,
			NumRows:        1,
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					FieldId:   1,
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{42}},
							},
						},
					},
				},
			},
			RowIDs:     []int64{42},
			Timestamps: []uint64{1},
		},
	}
}

// buildMutationResult creates a MutationResult with a single int64 PK row.
func buildMutationResult(pk int64) *milvuspb.MutationResult {
	return &milvuspb.MutationResult{
		IDs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}},
		},
	}
}

// TestRepackInsert_SetsRoutingVersion verifies that repackInsertDataForStreamingService
// stamps routing.CompatVersion on the produced message header.
func TestRepackInsert_SetsRoutingVersion(t *testing.T) {
	oldCache := globalMetaCache
	cache := NewMockCache(t)
	cache.On("GetPartitionID", mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	insertMsg := buildMinimalInsertMsg(100, "db", "coll", "_default")
	result := buildMutationResult(42)

	msgs, err := repackInsertDataForStreamingService(
		context.Background(),
		[]string{"ch"},
		insertMsg,
		result,
		nil, // no encryption
		1,   // schemaVersion
	)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)

	got := message.MustAsMutableInsertMessageV1(msgs[0]).Header().GetRoutingVersion()
	assert.Equal(t, routing.CompatVersion, got)
}

// TestRepackInsertWithPartitionKey_SetsRoutingVersion verifies that
// repackInsertDataWithPartitionKeyForStreamingService also stamps routing.CompatVersion.
func TestRepackInsertWithPartitionKey_SetsRoutingVersion(t *testing.T) {
	oldCache := globalMetaCache
	cache := NewMockCache(t)
	// partition key mode resolves default partitions via GetPartitions, then
	// looks up each partition id. Use two default partitions so hashing works.
	cache.On("GetPartitions", mock.Anything, "db", "coll").Return(map[string]int64{"_default_0": 201, "_default_1": 202}, nil)
	cache.On("GetPartitionID", mock.Anything, "db", "coll", "_default_0").Return(int64(201), nil)
	cache.On("GetPartitionID", mock.Anything, "db", "coll", "_default_1").Return(int64(202), nil)
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	insertMsg := buildMinimalInsertMsg(100, "db", "coll", "")
	result := buildMutationResult(42)

	// partitionKeys: a single int64 value so HashKey2Partitions can route it.
	partitionKeys := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{42}},
				},
			},
		},
	}

	msgs, err := repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		[]string{"ch"},
		insertMsg,
		result,
		partitionKeys,
		nil, // no encryption
		1,   // schemaVersion
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgs)

	for _, m := range msgs {
		got := message.MustAsMutableInsertMessageV1(m).Header().GetRoutingVersion()
		assert.Equal(t, routing.CompatVersion, got)
	}
}
