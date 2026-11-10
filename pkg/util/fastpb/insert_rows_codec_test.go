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

package fastpb

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestInsertRowsEncodingRoundtrip(t *testing.T) {
	for _, ids := range [][]int64{{0}, {1}, {-1}, {127, 128, 129}, {1 << 50, 1<<50 + 17, 1<<50 + 16, 1<<50 + 16}, {math.MinInt64, math.MaxInt64}, {math.MaxInt64, math.MinInt64}, {0, math.MaxInt64, 0}} {
		rows := &messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: ids}}}
		before := proto.Clone(rows)
		compact := CompactInsertRows(rows)
		require.LessOrEqual(t, proto.Size(compact), proto.Size(rows))
		require.True(t, proto.Equal(before, rows))
		encoded, err := proto.Marshal(&messagespb.InsertMessageBody{Rows: compact})
		require.NoError(t, err)
		decoded := &messagespb.InsertMessageBody{}
		require.NoError(t, proto.Unmarshal(encoded, decoded))
		require.NoError(t, NormalizeInsertOrUpsertBody(decoded))
		require.Equal(t, ids, decoded.Rows.GetRowIds().GetData())
	}
	for _, delta := range []*messagespb.DeltaEncodedRowIDs{{Base: math.MaxInt64, Deltas: []int64{1}}, {Base: math.MinInt64, Deltas: []int64{-1}}} {
		require.Error(t, NormalizeInsertOrUpsertBody(&messagespb.InsertMessageBody{Rows: &messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: delta}}}))
	}
}

func TestDeleteKeyReferencePreservesOccurrencesAndScope(t *testing.T) {
	for _, keys := range []*schemapb.IDs{
		{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 7, 9}}}},
		{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"七", "七", "九"}}}},
	} {
		effect := &messagespb.UpsertDeleteEffect{Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 42}, KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: proto.Clone(keys).(*schemapb.IDs)}}
		before := proto.Clone(effect)
		compact := CompactDeleteEffects(keys, []*messagespb.UpsertDeleteEffect{effect})
		require.NotNil(t, compact[0].GetAllInsertedKeys())
		require.Equal(t, int64(42), compact[0].GetPartitionId())
		require.True(t, proto.Equal(before, effect))
		body := &messagespb.UpsertMessageBody{Rows: &messagespb.InsertRows{PrimaryKeys: keys}, DeleteEffects: compact}
		require.NoError(t, NormalizeInsertOrUpsertBody(body))
		require.Same(t, keys, body.DeleteEffects[0].GetPrimaryKeys())
		independent := &messagespb.UpsertDeleteEffect{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{9, 7, 7}}}}}}
		require.Same(t, independent, CompactDeleteEffects(keys, []*messagespb.UpsertDeleteEffect{independent})[0])
	}
}

func BenchmarkInsertRowsWireEncoding(b *testing.B) {
	ids := make([]int64, 4096)
	for index := range ids {
		ids[index] = 1<<50 + int64(index*16)
	}
	rows := &messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: ids}}}
	for _, compact := range []bool{false, true} {
		name := "explicit"
		if compact {
			name = "delta"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				encoded := rows
				if compact {
					encoded = CompactInsertRows(rows)
				}
				wire, err := proto.Marshal(encoded)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(wire)), "wire-bytes")
			}
		})
	}
}

func TestInsertRowsEncodingMatchesProtobufSizeOracle(t *testing.T) {
	random := rand.New(rand.NewSource(11))
	for run := 0; run < 300; run++ {
		ids := make([]int64, 1+random.Intn(80))
		ids[0] = random.Int63()
		deltas := make([]int64, len(ids)-1)
		for index := range deltas {
			deltas[index] = int64(random.Intn(255) - 127)
			ids[index+1] = ids[index] + deltas[index]
		}
		explicit := &messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: ids}}}
		delta := &messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: &messagespb.DeltaEncodedRowIDs{Base: ids[0], Deltas: deltas}}}
		actual := CompactInsertRows(explicit)
		require.Equal(t, min(proto.Size(explicit), proto.Size(delta)), proto.Size(actual))
		require.Equal(t, proto.Size(delta) < proto.Size(explicit), actual.GetDeltaRowIds() != nil)
	}
}
