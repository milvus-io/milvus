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

package dml

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func upsertPackingSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, {FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true}, {FieldID: 102, Name: "payload", DataType: schemapb.DataType_VarChar}}}
}

func TestInsertOrUpsertPackingMultiplePartitionsAndOriginalOffsets(t *testing.T) {
	for _, typ := range []message.MessageType{message.MessageTypeInsert, message.MessageTypeUpsert} {
		t.Run(typ.String(), func(t *testing.T) {
			source, result := newIdempotentRepackInsertMsg(16)
			before := proto.Clone(source.InsertRequest)
			cache := NewMockCache(t)
			cache.EXPECT().GetPartitions(mock.Anything, "db", "coll").Return(map[string]int64{"partition_0": 300, "partition_1": 301}, nil)
			cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_0").Return(int64(300), nil)
			cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_1").Return(int64(301), nil)
			var msgs []message.MutableMessage
			var err error
			switch typ {
			case message.MessageTypeInsert:
				msgs, err = repackInsertMessages(t.Context(), cache, []string{"p_100v0"}, source, result.IDs, source.FieldsData[1], upsertPackingSchema(), 0, nil, "key")
			case message.MessageTypeUpsert:
				msgs, err = repackUpsertMessages(t.Context(), cache, []string{"p_100v0"}, source, result.IDs, source.FieldsData[1], upsertPackingSchema(), 0, nil, nil, "key", nil, 0)
			}
			require.NoError(t, err)
			require.Len(t, msgs, 1, "partitions must not split a VChannel write")
			require.Equal(t, typ, msgs[0].MessageType())
			var rows *messagespb.InsertRows
			switch typ {
			case message.MessageTypeInsert:
				insert := message.MustAsMutableInsertMessageV2(msgs[0])
				header := insert.Header()
				body := insert.MustBody()
				rows = body.Rows
				require.Len(t, header.Partitions, 2)
				require.NotNil(t, header.SchemaVersion)
				require.Zero(t, header.GetSchemaVersion())
				rowHeader, headerErr := message.ReadInsertOrUpsertHeader(insert)
				require.NoError(t, headerErr)
				projection, projectionErr := message.ReadInsertOrUpsertProjection(t.Context(), insert)
				require.NoError(t, projectionErr)
				_, err = message.ValidateInsertOrUpsertWithSelections(rowHeader, projection, upsertPackingSchema().Fields[0])
				require.NoError(t, err)
			case message.MessageTypeUpsert:
				upsert := message.MustAsMutableUpsertMessageV2(msgs[0])
				header := upsert.Header()
				body := upsert.MustBody()
				rows = body.Rows
				require.Len(t, header.Partitions, 2)
				require.NotNil(t, header.SchemaVersion)
				require.Zero(t, header.GetSchemaVersion())
				require.Empty(t, body.DeleteEffects, "row-only Upsert has no deletion")
				require.NoError(t, message.ValidateUpsert(header, body, upsertPackingSchema().Fields[0]))
			}
			require.Len(t, rows.PartitionIndexes, 16)
			require.Len(t, rows.Columns, 2)
			offsets := make([]uint32, 16)
			for i := range offsets {
				offsets[i] = uint32(i)
			}
			require.Equal(t, offsets, rows.RequestRowMapping.RowOffsets)
			require.Equal(t, result.IDs.GetIntId().Data, rows.PrimaryKeys.GetIntId().Data)
			require.True(t, proto.Equal(before, source.InsertRequest))
		})
	}
}

func TestUpsertPackingLargeKeyedWriteAndExplicitEffects(t *testing.T) {
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	oldLimit := limit.SwapTempValue("1048576")
	defer limit.SwapTempValue(oldLimit)
	source, result := newIdempotentRepackInsertMsgWithPayload(8, strings.Repeat("x", 300000))
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	deletion := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 3}}}}
	rows, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, nil, message.IdempotencyKey("large"), deletion, -1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var offsets []uint32
	var deleted []int64
	for _, raw := range rows {
		require.Equal(t, message.MessageTypeUpsert, raw.MessageType())
		require.Greater(t, raw.EstimateSize(), limit.GetAsInt())
		require.Equal(t, message.IdempotencyKey("large"), message.IdempotencyKeyOf(raw))
		typed := message.MustAsMutableUpsertMessageV2(raw)
		body := typed.MustBody()
		require.Empty(t, body.Rows.PartitionIndexes)
		require.NoError(t, message.ValidateUpsert(typed.Header(), body, upsertPackingSchema().Fields[0]))
		offsets = append(offsets, body.Rows.RequestRowMapping.RowOffsets...)
		for _, effect := range body.DeleteEffects {
			require.NotNil(t, effect.GetAllPartitions())
			deleted = append(deleted, effect.GetPrimaryKeys().GetIntId().Data...)
		}
	}
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7}, offsets)
	require.Equal(t, []int64{1, 3}, deleted)
}

func TestUpsertPackingCASAndNullableVector(t *testing.T) {
	source, result := newIdempotentRepackInsertMsg(3)
	source.FieldsData = append(source.FieldsData, &schemapb.FieldData{FieldId: 103, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, ValidData: []bool{true, false, true}, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 5, 6}}}}}})
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	proof := &messagespb.PartialUpdateCAS{}
	rows, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, map[string]*messagespb.PartialUpdateCAS{"p_100v0": proof}, "", nil, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.True(t, message.HasPartialUpdateCAS(rows[0]))
	body := message.MustAsMutableUpsertMessageV2(rows[0]).MustBody()
	require.True(t, proto.Equal(proof, body.PartialUpdateCas))
	require.Equal(t, []float32{1, 2, 5, 6}, body.Rows.Columns[2].GetVectors().GetFloatVector().Data)
	require.Equal(t, []bool{true, false, true}, body.Rows.Columns[2].GetVectors().ValidData)
}

func TestUpsertPackingAllowsOversizedLogicalMessage(t *testing.T) {
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	previous := limit.SwapTempValue("1048576")
	defer limit.SwapTempValue(previous)
	source, result := newIdempotentRepackInsertMsgWithPayload(1, strings.Repeat("x", 2<<20))
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, nil, "", nil, 0)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Greater(t, messages[0].EstimateSize(), limit.GetAsInt())
	keyed, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, nil, message.IdempotencyKey("bounded"), nil, 0)
	require.NoError(t, err)
	require.Len(t, keyed, 1)
	require.Greater(t, keyed[0].EstimateSize(), limit.GetAsInt())
}

func BenchmarkInsertOrUpsertProxyPacking(b *testing.B) {
	block, err := aes.NewCipher(make([]byte, 32))
	if err != nil {
		b.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		b.Fatal(err)
	}
	message.RegisterCipher(&upsertMessageTestCipher{aead: aead})
	for _, typ := range []message.MessageType{message.MessageTypeInsert, message.MessageTypeUpsert} {
		for _, partitions := range []bool{false, true} {
			for _, encrypted := range []bool{false, true} {
				for _, count := range []int{1024, 16384} {
					b.Run(fmt.Sprintf("%s/partitions_%v/encrypted_%v/rows_%d", typ, partitions, encrypted, count), func(b *testing.B) {
						source, result := newIdempotentRepackInsertMsgWithPayload(count, strings.Repeat("x", 128))
						cache := NewMockCache(b)
						var partitionKeys *schemapb.FieldData
						if partitions {
							partitionKeys = source.FieldsData[1]
							cache.EXPECT().GetPartitions(mock.Anything, "db", "coll").Return(map[string]int64{"partition_0": 300, "partition_1": 301}, nil)
							cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_0").Return(int64(300), nil)
							cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_1").Return(int64(301), nil)
						} else {
							cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
						}
						var config *message.CipherConfig
						if encrypted {
							config = &message.CipherConfig{EzID: 7, CollectionID: 10}
						}
						key := message.IdempotencyKey("bench")
						payloadBytes := 0
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							var msgs []message.MutableMessage
							var err error
							switch typ {
							case message.MessageTypeInsert:
								msgs, err = repackInsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, partitionKeys, upsertPackingSchema(), 1, config, key)
							case message.MessageTypeUpsert:
								msgs, err = repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, partitionKeys, upsertPackingSchema(), 1, config, nil, key, nil, 0)
							}
							if err != nil {
								b.Fatal(err)
							}
							if len(msgs) != 1 {
								b.Fatalf("expected one message, got %d", len(msgs))
							}
							payloadBytes += len(msgs[0].IntoMessageProto().Payload)
						}
						b.StopTimer()
						b.ReportMetric(1, "messages/op")
						b.ReportMetric(float64(payloadBytes)/float64(b.N), "payload_B/op")
					})
				}
			}
		}
	}
}

func TestUpsertPackingEncodingWork(t *testing.T) {
	if os.Getenv("MILVUS_TEST_UPSERT_PACKING_WORK") == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestUpsertPackingEncodingWork$", "-test.v") // #nosec G204 -- Re-exec the current test binary with a fixed filter.
		cmd.Env = append(os.Environ(), "MILVUS_TEST_UPSERT_PACKING_WORK=1")
		output, err := cmd.CombinedOutput()
		t.Logf("%s", output)
		require.NoError(t, err)
		return
	}
	block, err := aes.NewCipher(make([]byte, 32))
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	fixture := &upsertMessageTestCipher{aead: aead}
	message.RegisterCipher(fixture)
	for _, encrypted := range []bool{false, true} {
		for _, count := range []int{32, 128} {
			t.Run(fmt.Sprintf("encrypted_%v/rows_%d", encrypted, count), func(t *testing.T) {
				var original func(*fastpb.UpsertViewEncoder, []byte) (int, error)
				calls, encoded := 0, 0
				patch := mockey.Mock((*fastpb.UpsertViewEncoder).MarshalTo).Origin(&original).To(func(e *fastpb.UpsertViewEncoder, dst []byte) (int, error) {
					calls++
					encoded += len(dst)
					return original(e, dst)
				}).Build()
				defer patch.UnPatch()
				limit := &paramtable.Get().PulsarCfg.MaxMessageSize
				previous := limit.SwapTempValue("32768")
				defer limit.SwapTempValue(previous)
				source, result := newIdempotentRepackInsertMsgWithPayload(count, strings.Repeat("x", 4096))
				cache := NewMockCache(t)
				cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
				var config *message.CipherConfig
				if encrypted {
					config = &message.CipherConfig{EzID: 7, CollectionID: 10}
				}
				fixture.encryptCalls, fixture.encryptBytes = 0, 0
				messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, config, nil, message.IdempotencyKey("work"), nil, 0)
				require.NoError(t, err)
				require.Len(t, messages, 1)
				var offsets []uint32
				for _, msg := range messages {
					require.Greater(t, upsertWireSize(msg), limit.GetAsInt())
					body := message.MustAsMutableUpsertMessageV2(msg).MustBody()
					offsets = append(offsets, body.Rows.RequestRowMapping.RowOffsets...)
				}
				require.Len(t, offsets, count)
				for i, offset := range offsets {
					require.EqualValues(t, i, offset)
				}
				require.Equal(t, len(messages), calls, "each logical message is encoded exactly once")
				require.Less(t, encoded, count*4300, "encoded bytes scale with source rows")
				if encrypted {
					require.Equal(t, len(messages), fixture.encryptCalls)
					require.Equal(t, encoded, fixture.encryptBytes)
				}
				t.Logf("rows=%d messages=%d encoded_calls=%d encoded_bytes=%d encrypt_calls=%d encrypt_bytes=%d", count, len(messages), calls, encoded, fixture.encryptCalls, fixture.encryptBytes)
			})
		}
	}
}

func TestUpsertPackingDuplicateDeleteOccurrences(t *testing.T) {
	for _, stringsPK := range []bool{false, true} {
		const count = 1024
		source, result := newIdempotentRepackInsertMsgWithPayload(count, strings.Repeat("x", 1024))
		keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, count)}}}
		for i := range count {
			keys.GetIntId().Data[i] = int64(i % 4)
		}
		schema := upsertPackingSchema()
		if stringsPK {
			values := make([]string, count)
			for i := range count {
				values[i] = fmt.Sprint(i % 4)
			}
			keys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: values}}}
			schema.Fields[0].DataType = schemapb.DataType_VarChar
		}
		var original func(*schemapb.IDs, *schemapb.IDs, []uint32) (map[uint32][]int, error)
		calls, visits := 0, 0
		patch := mockey.Mock(matchUpsertDeletes).Origin(&original).To(func(inserted, deleted *schemapb.IDs, channels []uint32) (map[uint32][]int, error) {
			calls++
			visits += message.UpsertPrimaryKeyCount(inserted) + message.UpsertPrimaryKeyCount(deleted)
			return original(inserted, deleted, channels)
		}).Build()
		t.Cleanup(func() { patch.UnPatch() })
		plan, err := matchUpsertDeletes(keys, keys, make([]uint32, count))
		require.NoError(t, err)
		expanded := 0
		for _, offsets := range plan {
			expanded += len(offsets)
		}
		require.Equal(t, count, expanded)
		limit := &paramtable.Get().PulsarCfg.MaxMessageSize
		previous := limit.SwapTempValue("32768")
		defer limit.SwapTempValue(previous)
		cache := NewMockCache(t)
		cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
		result.IDs = keys
		messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, keys, nil, schema, 1, nil, nil, message.IdempotencyKey("duplicate"), keys, -1)
		require.NoError(t, err)
		require.Equal(t, 2, calls, "one direct check and one production match for the batch")
		require.Equal(t, 4*count, visits)
		t.Logf("string_pk=%v rows=%d matcher_calls=%d key_visits=%d", stringsPK, count, calls, visits)
		require.Len(t, messages, 1)
		var effects *schemapb.IDs
		deletes := 0
		for _, msg := range messages {
			body := message.MustAsMutableUpsertMessageV2(msg).MustBody()
			for _, effect := range body.DeleteEffects {
				if effects == nil {
					effects = effect.GetPrimaryKeys()
				} else {
					t.Fatal("all duplicate delete occurrences must belong to the logical message")
				}
				deletes += message.UpsertPrimaryKeyCount(effect.GetPrimaryKeys())
			}
		}
		require.Equal(t, count, deletes)
		require.True(t, proto.Equal(keys, effects))
		patch.UnPatch()
	}
}

func BenchmarkUpsertDuplicateMatching(b *testing.B) {
	for _, stringPK := range []bool{false, true} {
		for _, count := range []int{1024, 4096, 16384} {
			b.Run(fmt.Sprintf("string_%v/rows_%d", stringPK, count), func(b *testing.B) {
				keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, count)}}}
				if stringPK {
					values := make([]string, count)
					for i := range values {
						values[i] = "duplicate"
					}
					keys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: values}}}
				}
				channels := make([]uint32, count)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					plan, err := matchUpsertDeletes(keys, keys, channels)
					if err != nil || len(plan[0]) != count {
						b.Fatal("incomplete delete matching")
					}
				}
			})
		}
	}
}

func TestUpsertPackingLargeDeleteOccurrences(t *testing.T) {
	for _, stringPK := range []bool{false, true} {
		const rows = 512
		source, _ := newIdempotentRepackInsertMsgWithPayload(rows, strings.Repeat("x", 4096))
		ints := make([]int64, rows)
		texts := make([]string, rows)
		for i := range rows {
			ints[i] = int64(i / 64)
			texts[i] = fmt.Sprint(i / 64)
		}
		keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ints}}}
		schema := upsertPackingSchema()
		if stringPK {
			keys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: texts}}}
			schema.Fields[0].DataType = schemapb.DataType_VarChar
		}
		limit := &paramtable.Get().PulsarCfg.MaxMessageSize
		previous := limit.SwapTempValue("32768")
		defer limit.SwapTempValue(previous)
		cache := NewMockCache(t)
		cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
		messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, keys, nil, schema, 1, nil, nil, message.IdempotencyKey("boundary"), keys, -1)
		require.NoError(t, err)
		var gotInts []int64
		var gotStrings []string
		effectMessages := 0
		for _, msg := range messages {
			body := message.MustAsMutableUpsertMessageV2(msg).MustBody()
			if len(body.DeleteEffects) > 0 {
				effectMessages++
			}
			for _, effect := range body.DeleteEffects {
				gotInts = append(gotInts, effect.GetPrimaryKeys().GetIntId().GetData()...)
				gotStrings = append(gotStrings, effect.GetPrimaryKeys().GetStrId().GetData()...)
			}
		}
		require.Equal(t, 1, effectMessages)
		if stringPK {
			require.Equal(t, texts, gotStrings)
		} else {
			require.Equal(t, ints, gotInts)
		}
	}
}

func TestUpsertCASProofOnLargeMessage(t *testing.T) {
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	previousLimit := limit.SwapTempValue("131072")
	defer limit.SwapTempValue(previousLimit)
	proof := &messagespb.PartialUpdateCAS{ReadTs: 1, ObservedPchannelTerm: 2}
	for _, kind := range []string{"valid", "missing", "nil"} {
		t.Run(kind, func(t *testing.T) {
			source, result := newIdempotentRepackInsertMsgWithPayload(256, strings.Repeat("x", 4096))
			cache := NewMockCache(t)
			cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
			proofs := map[string]*messagespb.PartialUpdateCAS{}
			switch kind {
			case "valid":
				proofs["p_100v0"] = proof
			case "nil":
				proofs["p_100v0"] = nil
			}
			msgs, err := repackUpsertMessages(t.Context(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, proofs, "", nil, 0)
			if kind != "valid" {
				require.ErrorContains(t, err, "missing partial update CAS proof")
				return
			}
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			for _, msg := range msgs {
				require.True(t, message.HasPartialUpdateCAS(msg))
				require.True(t, proto.Equal(proof, message.MustAsMutableUpsertMessageV2(msg).MustBody().GetPartialUpdateCas()))
			}
		})
	}
}

func upsertWireSize(msg message.MutableMessage) int {
	wire := msg.IntoMessageProto()
	size := len(wire.Payload)
	for key, value := range wire.Properties {
		size += len(key) + len(value)
	}
	return size
}
