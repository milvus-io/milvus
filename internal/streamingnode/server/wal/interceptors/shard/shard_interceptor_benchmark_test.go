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
	"fmt"
	"maps"
	"testing"

	"github.com/bytedance/mockey"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// This benchmark includes admission and function-adapter work, not runner execution.
func BenchmarkInsertOrUpsertAdmission(b *testing.B) {
	paramtable.Init()
	const count = 1024
	for _, mode := range []string{"disabled", "outputs_present", "outputs_added"} {
		b.Run(mode, func(b *testing.B) {
			gate := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
			value := "true"
			if mode == "disabled" {
				value = "false"
			}
			old := gate.SwapTempValue(value)
			defer gate.SwapTempValue(old)
			output := &schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, count*128)}}}}}
			manager := &upsertMaterializationManager{materialize: func(_ context.Context, _ int64, _ string, _ int32, insert function.InsertMessage) (bool, error) {
				body := insert.MustBody()
				if function.HasAllFieldDataByID(body.FieldsData, []int64{102}) {
					return false, nil
				}
				body.FieldsData = append(body.FieldsData, output)
				insert.OverwriteBody(body)
				return true, nil
			}}
			patch := mockey.Mock(function.GetManager).Return(manager).Build()
			defer patch.UnPatch()
			for _, partitions := range []int{1, 8} {
				b.Run(fmt.Sprintf("partitions_%d", partitions), func(b *testing.B) {
					body := &message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, count)}}}, RowIdsEncoding: &message.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: make([]int64, count)}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, count*128)}}}}}}}}
					if mode == "outputs_present" {
						body.Rows.Columns = append(body.Rows.Columns, output)
					}
					header := &message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: proto.Int32(1)}
					for i := 0; i < partitions; i++ {
						header.Partitions = append(header.Partitions, &message.PartitionSegmentAssignment{PartitionId: int64(i + 1), Rows: count / uint64(partitions)})
					}
					if partitions > 1 {
						body.Rows.PartitionIndexes = make([]uint32, count)
						for i := range body.Rows.PartitionIndexes {
							body.Rows.PartitionIndexes[i] = uint32(i % partitions)
						}
					}
					wire := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(header).WithBody(body).MustBuildMutable().IntoMessageProto()
					impl := &shardInterceptor{shardManager: &admissionBenchmarkManager{schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}}}
					appendOp := func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, nil }
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						msg := message.NewMutableMessageBeforeAppend(wire.Payload, maps.Clone(wire.Properties)).WithTimeTick(10)
						if _, err := impl.handleInsertOrUpsert(context.Background(), msg, appendOp); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

type admissionBenchmarkManager struct {
	shards.ShardManager
	schema *schemapb.CollectionSchema
}

func (m *admissionBenchmarkManager) GetWriteSchema(int64, string) (*schemapb.CollectionSchema, error) {
	return m.schema, nil
}

func (m *admissionBenchmarkManager) CheckIfCollectionSchemaVersionMatch(*message.InsertMessageHeader) (int32, error) {
	return 1, nil
}

func (m *admissionBenchmarkManager) AssignSegments(requests []*shards.AssignSegmentRequest) ([]*shards.AssignSegmentResult, error) {
	results := make([]*shards.AssignSegmentResult, len(requests))
	for i := range results {
		results[i] = &shards.AssignSegmentResult{SegmentID: int64(i + 1), Acknowledge: atomic.NewInt32(1)}
	}
	return results, nil
}
