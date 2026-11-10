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
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/github.com/milvus-io/milvus-proto/go-api/v3/mock_hook"
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
	rows := &InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102}}}}, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1, 2}}}, RequestRowMapping: &RequestRowMapping{RowOffsets: []uint32{4, 0}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}}}}
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
			require.Equal(t, [][]int64{nil}, selections)
			result, err := BuildInsertOrUpsertResult(t.Context(), immutable)
			require.NoError(t, err)
			require.Equal(t, []uint32{4, 0}, result.RowOffsets)
			require.Equal(t, []int64{101, 102}, result.Ids.GetIntId().Data)
			require.Empty(t, projection.GetDeleteEffects())
		})
	}
}

func TestInsertOrUpsertProjectionMergesRepeatedRows(t *testing.T) {
	first := &InsertRows{RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1}}}, Columns: []*schemapb.FieldData{{FieldId: 101}}}
	second := &InsertRows{RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{2}}}, Columns: []*schemapb.FieldData{{FieldId: 102}}}
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
	payload, err := proto.Marshal(&UpsertMessageBody{Rows: &InsertRows{RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1}}}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: &schemapb.IDs{}}}}})
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
		payload, err := proto.Marshal(&UpsertMessageBody{Rows: &InsertRows{RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1}}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(bytes / 4), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, bytes/4)}}}}}}}})
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

func TestProjectionMergesWireFragmentsWithoutCopyingColumnValues(t *testing.T) {
	for _, keys := range []*schemapb.IDs{
		{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{-1, 128}}}},
		{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"", "你好"}}}},
	} {
		var payload []byte
		for _, fragment := range []*UpsertMessageBody{
			{Rows: &InsertRows{PrimaryKeys: keys, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1}}}, PartitionIndexes: []uint32{0}, RequestRowMapping: &RequestRowMapping{RowOffsets: []uint32{4}}, Columns: []*schemapb.FieldData{{FieldId: 101}}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: keys}, Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 10}}}, PartialUpdateCas: &messagespb.PartialUpdateCAS{}},
			{Rows: &InsertRows{PrimaryKeys: keys, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{2}}}, PartitionIndexes: []uint32{1}, RequestRowMapping: &RequestRowMapping{RowOffsets: []uint32{7}}, Columns: []*schemapb.FieldData{{FieldId: 102}}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: keys}, Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}}}, PartialUpdateCas: &messagespb.PartialUpdateCAS{}},
		} {
			encoded, err := proto.Marshal(fragment)
			require.NoError(t, err)
			payload = append(payload, encoded...)
		}
		expected := &UpsertMessageBody{}
		require.NoError(t, proto.Unmarshal(payload, expected))
		actual, err := decodeInsertOrUpsertProjection(payload, true)
		require.NoError(t, err)
		require.True(t, proto.Equal(expected, actual))
	}
}

func TestCompactRowsAndDeleteReferenceAcrossReaders(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "encrypted"}[encrypted], func(t *testing.T) {
			var config *CipherConfig
			if encrypted {
				original := cipher
				t.Cleanup(func() { cipher = original })
				c := mock_hook.NewMockCipher(t)
				e := mock_hook.NewMockEncryptor(t)
				d := mock_hook.NewMockDecryptor(t)
				e.EXPECT().Encrypt(mock.Anything).RunAndReturn(func(b []byte) ([]byte, error) { return append([]byte("enc"), b...), nil })
				d.EXPECT().Decrypt(mock.Anything).RunAndReturn(func(b []byte) ([]byte, error) { return b[3:], nil })
				c.EXPECT().GetEncryptor(mock.Anything, mock.Anything).Return(e, []byte("key"), nil)
				c.EXPECT().GetDecryptor(mock.Anything, mock.Anything, mock.Anything).Return(d, nil)
				cipher = c
				config = &CipherConfig{EzID: 1}
			}

			keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 7, 9}}}}
			rows := &InsertRows{PrimaryKeys: keys, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1 << 50, 1<<50 + 4, 1<<50 + 3}}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}}}}}
			effect := &messagespb.UpsertDeleteEffect{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: keys}}
			source := &UpsertMessageBody{Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{effect}}
			before := proto.Clone(source)
			rawMessage := NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&UpsertMessageHeader{DeleteRows: 3}).WithBody(source).WithCipher(config).MustBuildMutable()
			mutable, err := AsMutableUpsertMessageV2(rawMessage)
			require.NoError(t, err)
			assertWire := func() {
				raw := &UpsertMessageBody{}
				payload, err := rawMessage.(interface {
					decodePayload(context.Context) ([]byte, error)
				}).decodePayload(t.Context())
				require.NoError(t, err)
				require.NoError(t, proto.Unmarshal(payload, raw))
				require.NotNil(t, raw.Rows.GetDeltaRowIds())
				_, accounted, err := decodeProtoBWithSize[*UpsertMessageBody](t.Context(), rawMessage.(*messageImpl))
				require.NoError(t, err)
				require.Equal(t, len(payload)+8*len(rows.GetRowIds().GetData()), accounted)
				require.NotNil(t, raw.DeleteEffects[0].GetAllInsertedKeys())
			}
			assertWire()
			projection, err := ReadInsertOrUpsertProjection(t.Context(), mutable)
			require.NoError(t, err)
			require.Equal(t, rows.GetRowIds().GetData(), projection.Rows.GetRowIds().GetData())
			require.Same(t, projection.Rows.PrimaryKeys, projection.DeleteEffects[0].GetPrimaryKeys())
			require.Nil(t, projection.Rows.Columns[0].Field)
			decoded, err := mutable.Body(t.Context())
			require.NoError(t, err)
			require.True(t, proto.Equal(source, decoded))
			mutable.OverwriteBody(decoded)
			assertWire()
			require.NoError(t, ReplaceUpsertDeleteEffects(t.Context(), mutable, source.DeleteEffects))
			assertWire()
			projection, err = ReadInsertOrUpsertProjection(t.Context(), mutable)
			require.NoError(t, err)
			require.Equal(t, uint64(3), mutable.Header().DeleteRows)
			require.Same(t, projection.Rows.PrimaryKeys, projection.DeleteEffects[0].GetPrimaryKeys())
			require.True(t, proto.Equal(before, source))
			owned, err := mutable.Body(t.Context())
			require.NoError(t, err)
			owned.Rows.PrimaryKeys.GetIntId().Data = append(owned.Rows.PrimaryKeys.GetIntId().Data, 10)
			require.Equal(t, []int64{7, 7, 9}, owned.DeleteEffects[0].GetPrimaryKeys().GetIntId().Data)
			owned.DeleteEffects[0].GetPrimaryKeys().GetIntId().Data[0] = 99
			require.Equal(t, int64(7), owned.Rows.PrimaryKeys.GetIntId().Data[0])
		})
	}
}

func TestOverflowingDeltaRowsDoNotPublishBodyOrRetryResult(t *testing.T) {
	raw := NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&UpsertMessageHeader{}).WithBody(&UpsertMessageBody{Rows: &InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 8}}}}, RowIdsEncoding: &InsertRows_DeltaRowIds{DeltaRowIds: &DeltaEncodedRowIDs{Base: math.MaxInt64, Deltas: []int64{1}}}, RequestRowMapping: &RequestRowMapping{RowOffsets: []uint32{0, 1}}}}).MustBuildMutable()
	typed := MustAsMutableUpsertMessageV2(raw)
	body, err := typed.Body(t.Context())
	require.Nil(t, body)
	require.True(t, errors.Is(err, ErrMalformedBody))
	for range 2 {
		projection, err := ReadInsertOrUpsertProjection(t.Context(), raw)
		require.Nil(t, projection)
		require.True(t, errors.Is(err, ErrMalformedBody))
		result, err := BuildInsertOrUpsertResult(t.Context(), raw)
		require.Nil(t, result)
		require.Error(t, err)
	}
}
