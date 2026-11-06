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

package message_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func upsertFixture() (*message.UpsertMessageHeader, *message.UpsertMessageBody, *schemapb.FieldSchema) {
	v := int32(0)
	return &message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &v, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2}, {PartitionId: 20, Rows: 1}}}, &message.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4, 5, 4}}}}, RowIds: []int64{1, 2, 3}, PartitionIndexes: []uint32{0, 1, 0}}, &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
}

func TestUpsertContract(t *testing.T) {
	h, b, pk := upsertFixture()
	require.NoError(t, message.ValidateUpsert(h, b, pk))
	rows, err := message.UpsertSelections(h, b)
	require.NoError(t, err)
	require.Equal(t, [][]int64{{0, 2}, {1}}, rows)
	b.DeleteEffects = []*messagespb.UpsertDeleteEffect{{Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 30}, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{99}}}}}}
	h.DeleteRows = 1
	require.NoError(t, message.ValidateUpsert(h, b, pk))
	h.DeleteRows = 2
	require.Error(t, message.ValidateUpsert(h, b, pk))
	h.DeleteRows = 1
	b.PartitionIndexes[1] = 2
	require.Error(t, message.ValidateUpsert(h, b, pk))
}

func TestUpsertProjectionSkipsColumnsAndSharesDecode(t *testing.T) {
	h, b, _ := upsertFixture()
	b.RequestRowMapping = &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{0, 1, 2}}
	payload, err := proto.Marshal(b)
	require.NoError(t, err)
	column := protowire.AppendTag(nil, 5, protowire.VarintType)
	column = protowire.AppendVarint(column, 101)
	column = protowire.AppendTag(column, 4, protowire.BytesType)
	column = protowire.AppendBytes(column, []byte{0xff})
	payload = protowire.AppendTag(payload, 4, protowire.BytesType)
	payload = protowire.AppendBytes(payload, column)
	template := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(h).WithBody(b).MustBuildMutable()
	raw := message.NewImmutableMesasge(walimplstest.NewTestMessageID(1), payload, template.IntoMessageProto().Properties)
	first, err := message.ReadUpsertProjection(t.Context(), raw)
	require.NoError(t, err)
	second, err := message.ReadUpsertProjection(t.Context(), message.MustAsImmutableUpsertMessageV2(raw))
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Nil(t, first.Columns[0].Field)
	result, err := message.BuildUpsertResult(t.Context(), raw)
	require.NoError(t, err)
	require.Equal(t, []int64{4, 5, 4}, result.Ids.GetIntId().GetData())
	_, err = message.MustAsImmutableUpsertMessageV2(raw).Body(t.Context())
	require.Error(t, err)
}

func TestUpsertReplacementInvalidatesProjection(t *testing.T) {
	h, b, _ := upsertFixture()
	msg := message.MustAsMutableUpsertMessageV2(message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(h).WithBody(b).MustBuildMutable())
	before, err := message.ReadUpsertProjection(t.Context(), msg)
	require.NoError(t, err)
	encoded := msg.IntoMessageProto().Payload
	effect := &messagespb.UpsertDeleteEffect{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, PrimaryKeys: b.PrimaryKeys}
	require.NoError(t, message.ReplaceUpsertDeleteEffects(t.Context(), msg, []*messagespb.UpsertDeleteEffect{effect}))
	after, err := message.ReadUpsertProjection(t.Context(), msg)
	require.NoError(t, err)
	require.NotSame(t, before, after)
	require.Empty(t, before.DeleteEffects)
	require.Len(t, after.DeleteEffects, 1)
	require.Equal(t, uint64(3), msg.Header().DeleteRows)
	require.Equal(t, encoded, msg.IntoMessageProto().Payload[:len(encoded)])
}

func TestUpsertProjectionConcurrentReadersAndBodyReplacement(t *testing.T) {
	header, body, _ := upsertFixture()
	rawMutable := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(header).WithBody(body).MustBuildMutable()
	mutable := message.MustAsMutableUpsertMessageV2(rawMutable)
	raw := rawMutable.WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	first, err := message.ReadUpsertProjection(t.Context(), raw)
	require.NoError(t, err)
	var readers sync.WaitGroup
	for i := 0; i < 16; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 20; j++ {
				projection, err := message.ReadUpsertProjection(t.Context(), raw)
				require.NoError(t, err)
				require.Same(t, first, projection)
			}
		}()
	}
	readers.Wait()
	replacement := proto.Clone(body).(*message.UpsertMessageBody)
	replacement.PrimaryKeys.GetIntId().Data[0] = 99
	mutable.OverwriteBody(replacement)
	current, err := message.ReadUpsertProjection(t.Context(), mutable)
	require.NoError(t, err)
	require.Equal(t, int64(99), current.PrimaryKeys.GetIntId().Data[0])
	require.Equal(t, int64(4), first.PrimaryKeys.GetIntId().Data[0])
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = message.ReadUpsertProjection(ctx, raw)
	require.ErrorIs(t, err, context.Canceled)
}

func TestUpsertRejectsIncompleteContract(t *testing.T) {
	tests := map[string]func(*message.UpsertMessageHeader, *message.UpsertMessageBody){
		"missing schema": func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody) { h.SchemaVersion = nil },
		"missing RowID":  func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) { b.RowIds = b.RowIds[:1] },
		"wrong PK type": func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) {
			b.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b", "c"}}}}
		},
		"missing partition indexes": func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) { b.PartitionIndexes = nil },
		"duplicate partition": func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody) {
			h.Partitions[1].PartitionId = h.Partitions[0].PartitionId
		},
		"incorrect partition count": func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody) { h.Partitions[0].Rows++ },
		"invalid partition":         func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody) { h.Partitions[1].PartitionId = 0 },
		"system column": func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) {
			b.Columns = []*schemapb.FieldData{{FieldId: 1}}
		},
		"PK column": func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) {
			b.Columns = []*schemapb.FieldData{{FieldId: 100}}
		},
		"duplicate columns": func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) {
			b.Columns = []*schemapb.FieldData{{FieldId: 101}, {FieldId: 101}}
		},
		"incomplete mapping": func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody) {
			b.RequestRowMapping = &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{0}}
		},
		"missing delete scope": func(h *message.UpsertMessageHeader, b *message.UpsertMessageBody) {
			h.DeleteRows = 3
			b.DeleteEffects = []*messagespb.UpsertDeleteEffect{{PrimaryKeys: b.PrimaryKeys}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			header, body, primary := upsertFixture()
			mutate(header, body)
			selections, err := message.ValidateUpsertWithSelections(header, body, primary)
			require.Error(t, err)
			require.Nil(t, selections)
		})
	}
}

func TestUpsertSelectionsShareProjectionAcrossReaders(t *testing.T) {
	header, body, _ := upsertFixture()
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(header).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(42))
	first, err := message.ReadUpsertRowSelections(t.Context(), raw)
	require.NoError(t, err)
	second, err := message.ReadUpsertRowSelections(t.Context(), message.MustAsImmutableUpsertMessageV2(raw))
	require.NoError(t, err)
	require.Equal(t, &first[0][0], &second[0][0], "segment readers share the immutable row selection")
	positions, err := message.ReadUpsertCompactRows(t.Context(), raw, 101, []bool{true, false, true})
	require.NoError(t, err)
	again, err := message.ReadUpsertCompactRows(t.Context(), raw, 101, []bool{true, false, true})
	require.NoError(t, err)
	require.Equal(t, &positions[0], &again[0])
	require.Equal(t, []int64{0, -1, 1}, positions)
}

func BenchmarkUpsertValidationSelections(b *testing.B) {
	for _, shared := range []bool{false, true} {
		name := "duplicate"
		if shared {
			name = "shared"
		}
		b.Run(name, func(b *testing.B) {
			header, body, primary := upsertFixture()
			const count = 4096
			header.Partitions[0].Rows = count / 2
			header.Partitions[1].Rows = count / 2
			body.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, count)}}}
			body.RowIds = make([]int64, count)
			body.PartitionIndexes = make([]uint32, count)
			for i := range body.PartitionIndexes {
				body.PartitionIndexes[i] = uint32(i % 2)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if shared {
					if _, err := message.ValidateUpsertWithSelections(header, body, primary); err != nil {
						b.Fatal(err)
					}
				} else {
					if err := message.ValidateUpsert(header, body, primary); err != nil {
						b.Fatal(err)
					}
					if _, err := message.UpsertSelections(header, body); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
