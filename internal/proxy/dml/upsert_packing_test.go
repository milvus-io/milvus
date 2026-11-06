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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func upsertPackingSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, {FieldID: 101, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true}, {FieldID: 102, Name: "payload", DataType: schemapb.DataType_VarChar}}}
}

func TestUpsertPackingMultiplePartitionsAndOriginalOffsets(t *testing.T) {
	source, result := newIdempotentRepackInsertMsg(16)
	before := proto.Clone(source.InsertRequest)
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitions(mock.Anything, "db", "coll").Return(map[string]int64{"partition_0": 300, "partition_1": 301}, nil)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_0").Return(int64(300), nil)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_1").Return(int64(301), nil)
	key := &insertIdempotencyDecoration{key: "key"}
	rows, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, source.FieldsData[1], upsertPackingSchema(), 0, nil, nil, key, nil, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1, "partitions must not split a VChannel write")
	upsert := message.MustAsMutableUpsertMessageV2(rows[0])
	require.Len(t, upsert.Header().Partitions, 2)
	require.NotNil(t, upsert.Header().SchemaVersion)
	require.Zero(t, upsert.Header().GetSchemaVersion())
	body := upsert.MustBody()
	require.Len(t, body.PartitionIndexes, 16)
	require.Len(t, body.Columns, 2)
	require.Empty(t, body.DeleteEffects, "ordinary Insert has no deletion")
	offsets := make([]uint32, 16)
	for i := range offsets {
		offsets[i] = uint32(i)
	}
	require.Equal(t, offsets, body.RequestRowMapping.RowOffsets)
	require.Equal(t, result.IDs.GetIntId().Data, body.PrimaryKeys.GetIntId().Data)
	require.NoError(t, message.ValidateUpsert(upsert.Header(), body, upsertPackingSchema().Fields[0]))
	require.True(t, proto.Equal(before, source.InsertRequest))
}

func TestUpsertPackingSplitRetryAndExplicitEffects(t *testing.T) {
	split := &paramtable.Get().ProxyCfg.SplitChunkProxy
	previous := split.SwapTempValue("true")
	defer split.SwapTempValue(previous)
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	oldLimit := limit.SwapTempValue("1048576")
	defer limit.SwapTempValue(oldLimit)
	source, result := newIdempotentRepackInsertMsgWithPayload(8, strings.Repeat("x", 300000))
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	deletion := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 3}}}}
	rows, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, nil, &insertIdempotencyDecoration{key: "split"}, deletion, -1)
	require.NoError(t, err)
	require.Greater(t, len(rows), 1)
	var offsets []uint32
	var deleted []int64
	for _, raw := range rows {
		require.Equal(t, message.MessageTypeUpsert, raw.MessageType())
		require.LessOrEqual(t, raw.EstimateSize(), limit.GetAsInt())
		require.Equal(t, message.IdempotencyKey("split"), message.IdempotencyKeyOf(raw))
		typed := message.MustAsMutableUpsertMessageV2(raw)
		body := typed.MustBody()
		require.Empty(t, body.PartitionIndexes)
		require.NoError(t, message.ValidateUpsert(typed.Header(), body, upsertPackingSchema().Fields[0]))
		offsets = append(offsets, body.RequestRowMapping.RowOffsets...)
		for _, effect := range body.DeleteEffects {
			require.NotNil(t, effect.GetAllPartitions())
			deleted = append(deleted, effect.PrimaryKeys.GetIntId().Data...)
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
	rows, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, map[string]*messagespb.PartialUpdateCAS{"p_100v0": proof}, nil, nil, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.True(t, message.HasPartialUpdateCAS(rows[0]))
	body := message.MustAsMutableUpsertMessageV2(rows[0]).MustBody()
	require.True(t, proto.Equal(proof, body.PartialUpdateCas))
	require.Equal(t, []float32{1, 2, 5, 6}, body.Columns[2].GetVectors().GetFloatVector().Data)
	require.Equal(t, []bool{true, false, true}, body.Columns[2].GetVectors().ValidData)
}

func TestUpsertPackingOversizedPlainRowUsesWALChunking(t *testing.T) {
	split := &paramtable.Get().ProxyCfg.SplitChunkProxy
	old := split.SwapTempValue("true")
	defer split.SwapTempValue(old)
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	previous := limit.SwapTempValue("1048576")
	defer limit.SwapTempValue(previous)
	source, result := newIdempotentRepackInsertMsgWithPayload(1, strings.Repeat("x", 2<<20))
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, nil, nil, nil, 0)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Greater(t, messages[0].EstimateSize(), limit.GetAsInt())
	_, err = repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, nil, &insertIdempotencyDecoration{key: "bounded"}, nil, 0)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func BenchmarkUpsertProxyPacking(b *testing.B) {
	for _, partitions := range []bool{false, true} {
		name := "single_partition"
		if partitions {
			name = "partition_key"
		}
		b.Run(name, func(b *testing.B) {
			source, result := newIdempotentRepackInsertMsgWithPayload(1024, strings.Repeat("x", 128))
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
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, partitionKeys, upsertPackingSchema(), 1, nil, nil, &insertIdempotencyDecoration{key: "bench"}, nil, 0)
				if err != nil {
					b.Fatal(err)
				}
				if len(messages) != 1 {
					b.Fatalf("expected one message, got %d", len(messages))
				}
			}
		})
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
				split := &paramtable.Get().ProxyCfg.SplitChunkProxy
				old := split.SwapTempValue("true")
				defer split.SwapTempValue(old)
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
				messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, config, nil, &insertIdempotencyDecoration{key: "work"}, nil, 0)
				require.NoError(t, err)
				require.Greater(t, len(messages), 2)
				var offsets []uint32
				for _, msg := range messages {
					require.LessOrEqual(t, upsertWireSize(msg), limit.GetAsInt())
					body := message.MustAsMutableUpsertMessageV2(msg).MustBody()
					offsets = append(offsets, body.RequestRowMapping.RowOffsets...)
				}
				require.Len(t, offsets, count)
				for i, offset := range offsets {
					require.EqualValues(t, i, offset)
				}
				require.Equal(t, len(messages), calls, "each accepted chunk is encoded exactly once")
				require.Less(t, encoded, count*4300, "encoded bytes scale with source rows")
				if encrypted {
					require.Equal(t, len(messages)+1, fixture.encryptCalls)
					require.Equal(t, encoded, fixture.encryptBytes)
				}
				t.Logf("rows=%d chunks=%d encoded_calls=%d encoded_bytes=%d encrypt_calls=%d encrypt_bytes=%d", count, len(messages), calls, encoded, fixture.encryptCalls, fixture.encryptBytes)
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
		var original func(*schemapb.IDs, *schemapb.IDs) ([][]int, error)
		calls, visits := 0, 0
		patch := mockey.Mock(matchUpsertDeletes).Origin(&original).To(func(inserted, deleted *schemapb.IDs) ([][]int, error) {
			calls++
			visits += message.UpsertPrimaryKeyCount(inserted) + message.UpsertPrimaryKeyCount(deleted)
			return original(inserted, deleted)
		}).Build()
		t.Cleanup(func() { patch.UnPatch() })
		plan, err := matchUpsertDeletes(keys, keys)
		require.NoError(t, err)
		expanded := 0
		for row, offsets := range plan {
			expanded += len(offsets)
			if row >= 4 {
				require.Empty(t, offsets)
			}
		}
		require.Equal(t, count, expanded)
		split := &paramtable.Get().ProxyCfg.SplitChunkProxy
		old := split.SwapTempValue("true")
		defer split.SwapTempValue(old)
		limit := &paramtable.Get().PulsarCfg.MaxMessageSize
		previous := limit.SwapTempValue("32768")
		defer limit.SwapTempValue(previous)
		cache := NewMockCache(t)
		cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
		result.IDs = keys
		messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, keys, nil, schema, 1, nil, nil, &insertIdempotencyDecoration{key: "duplicate"}, keys, -1)
		require.NoError(t, err)
		require.Equal(t, 2, calls, "one direct check and one production match for all chunks")
		require.Equal(t, 4*count, visits)
		t.Logf("string_pk=%v rows=%d matcher_calls=%d key_visits=%d", stringsPK, count, calls, visits)
		require.Greater(t, len(messages), 2)
		var effects *schemapb.IDs
		deletes := 0
		for _, msg := range messages {
			body := message.MustAsMutableUpsertMessageV2(msg).MustBody()
			for _, effect := range body.DeleteEffects {
				if effects == nil {
					effects = effect.PrimaryKeys
				} else {
					t.Fatal("all duplicate delete occurrences must belong to the first chunk")
				}
				deletes += message.UpsertPrimaryKeyCount(effect.PrimaryKeys)
			}
		}
		require.Equal(t, count, deletes)
		require.True(t, proto.Equal(keys, effects))
		patch.UnPatch()
	}
}

func BenchmarkUpsertSplitScaling(b *testing.B) {
	block, err := aes.NewCipher(make([]byte, 32))
	if err != nil {
		b.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		b.Fatal(err)
	}
	fixture := &upsertMessageTestCipher{aead: aead}
	message.RegisterCipher(fixture)
	split := &paramtable.Get().ProxyCfg.SplitChunkProxy
	old := split.SwapTempValue("true")
	defer split.SwapTempValue(old)
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	previous := limit.SwapTempValue("32768")
	defer limit.SwapTempValue(previous)
	for _, encrypted := range []bool{false, true} {
		for _, count := range []int{32, 128, 512} {
			b.Run(fmt.Sprintf("encrypted_%v/rows_%d", encrypted, count), func(b *testing.B) {
				source, result := newIdempotentRepackInsertMsgWithPayload(count, strings.Repeat("x", 4096))
				cache := NewMockCache(b)
				cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
				var config *message.CipherConfig
				if encrypted {
					config = &message.CipherConfig{EzID: 7, CollectionID: 10}
				}
				var original func(*fastpb.UpsertViewEncoder, []byte) (int, error)
				calls, encoded, chunks := 0, 0, 0
				patch := mockey.Mock((*fastpb.UpsertViewEncoder).MarshalTo).Origin(&original).To(func(e *fastpb.UpsertViewEncoder, dst []byte) (int, error) {
					calls++
					encoded += len(dst)
					return original(e, dst)
				}).Build()
				defer patch.UnPatch()
				fixture.encryptCalls, fixture.encryptBytes = 0, 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, config, nil, &insertIdempotencyDecoration{key: "bench"}, nil, 0)
					if err != nil {
						b.Fatal(err)
					}
					chunks += len(messages)
				}
				b.StopTimer()
				b.ReportMetric(float64(calls)/float64(b.N), "encodes/op")
				b.ReportMetric(float64(encoded)/float64(b.N), "encoded_B/op")
				b.ReportMetric(float64(chunks)/float64(b.N), "chunks/op")
				b.ReportMetric(float64(fixture.encryptBytes)/float64(b.N), "encrypted_B/op")
			})
		}
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
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					plan, err := matchUpsertDeletes(keys, keys)
					if err != nil || len(plan[0]) != count {
						b.Fatal("incomplete delete matching")
					}
				}
			})
		}
	}
}

func TestUpsertPackingDeleteOccurrencesAcrossChunkBoundaries(t *testing.T) {
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
		split := &paramtable.Get().ProxyCfg.SplitChunkProxy
		old := split.SwapTempValue("true")
		defer split.SwapTempValue(old)
		limit := &paramtable.Get().PulsarCfg.MaxMessageSize
		previous := limit.SwapTempValue("32768")
		defer limit.SwapTempValue(previous)
		cache := NewMockCache(t)
		cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
		messages, err := repackUpsertMessages(context.Background(), cache, []string{"p_100v0"}, source, keys, nil, schema, 1, nil, nil, &insertIdempotencyDecoration{key: "boundary"}, keys, -1)
		require.NoError(t, err)
		var gotInts []int64
		var gotStrings []string
		effectChunks := 0
		for _, msg := range messages {
			body := message.MustAsMutableUpsertMessageV2(msg).MustBody()
			if len(body.DeleteEffects) > 0 {
				effectChunks++
			}
			for _, effect := range body.DeleteEffects {
				gotInts = append(gotInts, effect.PrimaryKeys.GetIntId().GetData()...)
				gotStrings = append(gotStrings, effect.PrimaryKeys.GetStrId().GetData()...)
			}
		}
		require.Greater(t, effectChunks, 1)
		if stringPK {
			require.Equal(t, texts, gotStrings)
		} else {
			require.Equal(t, ints, gotInts)
		}
	}
}

func TestUpsertCASProofAcrossSizeChunks(t *testing.T) {
	split := &paramtable.Get().ProxyCfg.SplitChunkProxy
	previousSplit := split.SwapTempValue("true")
	defer split.SwapTempValue(previousSplit)
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
			msgs, err := repackUpsertMessages(t.Context(), cache, []string{"p_100v0"}, source, result.IDs, nil, upsertPackingSchema(), 1, nil, proofs, nil, nil, 0)
			if kind != "valid" {
				require.ErrorContains(t, err, "missing partial update CAS proof")
				return
			}
			require.NoError(t, err)
			require.Greater(t, len(msgs), 1)
			for _, msg := range msgs {
				require.True(t, message.HasPartialUpdateCAS(msg))
				require.True(t, proto.Equal(proof, message.MustAsMutableUpsertMessageV2(msg).MustBody().GetPartialUpdateCas()))
			}
		})
	}
}
