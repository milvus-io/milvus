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

package walview

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestWalkUpsertInsertPreservesBody(t *testing.T) {
	assignment := &messagespb.PartitionSegmentAssignment{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 20}}
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: func() *int32 { v := int32(1); return &v }(), Partitions: []*message.PartitionSegmentAssignment{assignment}, DeleteRows: 0}).
		WithBody(upsertfixture.Body(&msgpb.InsertRequest{
			Version:      msgpb.InsertDataVersion_ColumnBased,
			CollectionID: 1, PartitionID: 999, SegmentID: 888, RowIDs: []int64{4}, Timestamps: []uint64{7}, NumRows: 1,
			FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42}}}}}}},
		}, nil)).MustBuildMutable().WithTimeTick(30).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(30))
	before := proto.Clone(message.MustAsImmutableUpsertMessageV2(raw).MustBody())
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	calls := 0
	require.NoError(t, ForEachSegmentInsertMessage(raw, 20, func(insert SegmentInsertMessage) error {
		calls++
		require.Equal(t, uint64(30), insert.TimeTick)
		require.Equal(t, int64(20), insert.Assignment.GetSegmentAssignment().GetSegmentId())
		request, err := MaterializeInsertRequest(schema, insert)
		require.NoError(t, err)
		require.Equal(t, int64(10), request.PartitionID)
		require.Equal(t, int64(20), request.SegmentID)
		require.Equal(t, []int64{42}, request.GetFieldsData()[0].GetScalars().GetLongData().GetData())
		request.FieldsData[0].FieldName = "owned metadata"
		return nil
	}))
	require.Equal(t, 1, calls)
	require.NoError(t, ForEachSegmentInsertMessage(raw, 99, func(SegmentInsertMessage) error {
		t.Fatal("an Upsert must not copy its rows into another segment")
		return nil
	}))
	require.True(t, proto.Equal(before, message.MustAsImmutableUpsertMessageV2(raw).MustBody()))
}
