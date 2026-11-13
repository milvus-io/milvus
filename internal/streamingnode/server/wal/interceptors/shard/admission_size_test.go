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

package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type sizeAdmissionManager struct {
	admissionBenchmarkManager
	size uint64
}

func (m *sizeAdmissionManager) AssignSegments(requests []*shards.AssignSegmentRequest) ([]*shards.AssignSegmentResult, error) {
	m.size = requests[0].ModifiedMetrics.BinarySize
	return m.admissionBenchmarkManager.AssignSegments(requests)
}

func TestSinglePartitionAdmissionDoesNotDecodeColumnsOrTrustHeaderSize(t *testing.T) {
	paramtable.Init()
	item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
	old := item.SwapTempValue("false")
	defer item.SwapTempValue(old)
	for _, typ := range []message.MessageType{message.MessageTypeInsert, message.MessageTypeUpsert} {
		t.Run(typ.String(), func(t *testing.T) {
			rows := &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}, RowIdsEncoding: &message.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1 << 50}}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 1024, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 1024)}}}}}}}
			partitions := []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1, BinarySize: 1}}
			var msg message.MutableMessage
			var patch *mockey.MockBuilder
			switch typ {
			case message.MessageTypeInsert:
				msg = message.NewInsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeaderV2{CollectionId: 1, SchemaVersion: proto.Int32(1), Partitions: partitions}).WithBody(&message.InsertMessageBody{Rows: rows}).MustBuildMutable()
				patch = mockey.Mock(mockey.GetMethod(message.MustAsMutableInsertMessageV2(msg), "Body")).Return((*message.InsertMessageBody)(nil), merr.ErrIoPermissionDenied)
			case message.MessageTypeUpsert:
				msg = message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: proto.Int32(1), Partitions: partitions}).WithBody(&message.UpsertMessageBody{Rows: rows}).MustBuildMutable()
				patch = mockey.Mock(mockey.GetMethod(message.MustAsMutableUpsertMessageV2(msg), "Body")).Return((*message.UpsertMessageBody)(nil), merr.ErrIoPermissionDenied)
			}
			bodyPatch := patch.Build()
			defer bodyPatch.UnPatch()
			manager := &sizeAdmissionManager{admissionBenchmarkManager: admissionBenchmarkManager{schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}}}
			impl := &shardInterceptor{shardManager: manager}
			appended := false
			_, err := impl.handleInsertOrUpsert(t.Context(), msg.WithTimeTick(10), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return nil, nil
			})
			require.NoError(t, err)
			require.True(t, appended)
			require.Greater(t, manager.size, uint64(4096))
			require.Zero(t, bodyPatch.Times())
		})
	}
}
