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

package message

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestInsertVersionConversions(t *testing.T) {
	legacy := NewInsertMessageBuilderV1().WithVChannel("v").WithHeader(&InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
	current := NewInsertMessageBuilderV2().WithVChannel("v").WithHeader(&InsertMessageHeaderV2{}).WithBody(&InsertMessageBody{Rows: &InsertRows{}}).MustBuildMutable()
	_, err := AsMutableInsertMessageV2(legacy)
	require.Error(t, err)
	_, err = AsMutableInsertMessageV1(current)
	require.Error(t, err)
	_, err = AsImmutableInsertMessageV1(current.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil))
	require.Error(t, err)
	_, err = AsImmutableInsertMessageV2(legacy.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil))
	require.Error(t, err)
	_, err = AsMutableInsertMessageV1(legacy.WithOldVersion())
	require.NoError(t, err)
	_, err = AsImmutableInsertMessageV1(legacy.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil))
	require.NoError(t, err)
}

func TestInsertOrUpsertProjectionAndOriginalResult(t *testing.T) {
	schemaVersion := int32(0)
	rows := &InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102}}}}, RowIds: []int64{1, 2}, RequestRowMapping: &RequestRowMapping{RowOffsets: []uint32{4, 0}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}}}}
	for _, kind := range []MessageType{MessageTypeInsert, MessageTypeUpsert} {
		t.Run(kind.String(), func(t *testing.T) {
			partitions := []*PartitionSegmentAssignment{{PartitionId: 10, Rows: 2}}
			var mutable MutableMessage
			switch kind {
			case MessageTypeInsert:
				mutable = NewInsertMessageBuilderV2().WithVChannel("v").WithHeader(&InsertMessageHeaderV2{CollectionId: 1, Partitions: partitions, SchemaVersion: &schemaVersion}).WithBody(&InsertMessageBody{Rows: rows}).MustBuildMutable()
			case MessageTypeUpsert:
				mutable = NewUpsertMessageBuilderV2().WithVChannel("v").WithHeader(&UpsertMessageHeader{CollectionId: 1, Partitions: partitions, SchemaVersion: &schemaVersion}).WithBody(&UpsertMessageBody{Rows: rows}).MustBuildMutable()
			}
			immutable := mutable.WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil)
			projection, err := ReadInsertOrUpsertProjection(t.Context(), immutable)
			require.NoError(t, err)
			require.True(t, proto.Equal(rows.PrimaryKeys, projection.GetRows().PrimaryKeys))
			require.Nil(t, projection.GetRows().Columns[0].Field)
			selections, err := ReadInsertOrUpsertRowSelections(t.Context(), immutable)
			require.NoError(t, err)
			require.Equal(t, [][]int64{{0, 1}}, selections)
			result, err := BuildInsertOrUpsertResult(t.Context(), immutable)
			require.NoError(t, err)
			require.Equal(t, []uint32{4, 0}, result.RowOffsets)
			require.Equal(t, []int64{101, 102}, result.Ids.GetIntId().Data)
			require.Empty(t, projection.GetDeleteEffects())
		})
	}
}

func TestInsertOrUpsertProjectionMergesRepeatedRows(t *testing.T) {
	first := &InsertRows{RowIds: []int64{1}, Columns: []*schemapb.FieldData{{FieldId: 101}}}
	second := &InsertRows{RowIds: []int64{2}, Columns: []*schemapb.FieldData{{FieldId: 102}}}
	var payload []byte
	for _, rows := range []*InsertRows{first, second} {
		encoded, err := proto.Marshal(rows)
		require.NoError(t, err)
		payload = protowire.AppendTag(payload, 1, protowire.BytesType)
		payload = protowire.AppendBytes(payload, encoded)
	}
	expected := &UpsertMessageBody{}
	require.NoError(t, proto.Unmarshal(payload, expected))
	actual, err := decodeInsertOrUpsertProjection(payload, true)
	require.NoError(t, err)
	require.True(t, proto.Equal(expected, actual))
}

func TestInsertProjectionIgnoresUpsertOnlyFields(t *testing.T) {
	// Insert's concrete body has no effects/proof fields. Unknown fields must
	// not acquire Upsert semantics through the common projection decoder.
	payload, err := proto.Marshal(&UpsertMessageBody{Rows: &InsertRows{RowIds: []int64{1}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{PrimaryKeys: &schemapb.IDs{}}}})
	require.NoError(t, err)
	projection, err := decodeInsertOrUpsertProjection(payload, false)
	require.NoError(t, err)
	require.Empty(t, projection.DeleteEffects)
	require.Nil(t, projection.PartialUpdateCas)
}

func BenchmarkInsertOrUpsertProjectionColumns(b *testing.B) {
	for _, bytes := range []int{0, 1 << 20, 8 << 20} {
		name := "empty"
		switch bytes {
		case 1 << 20:
			name = "1MiB"
		case 8 << 20:
			name = "8MiB"
		}
		payload, err := proto.Marshal(&UpsertMessageBody{Rows: &InsertRows{RowIds: []int64{1}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(bytes / 4), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, bytes/4)}}}}}}}})
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := decodeInsertOrUpsertProjection(payload, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
