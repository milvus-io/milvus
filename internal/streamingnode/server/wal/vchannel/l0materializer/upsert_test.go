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

package l0materializer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func walUpsert(tt uint64) message.ImmutableMessage {
	return message.NewUpsertMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 1}).
		WithBody(upsertfixture.Body(&msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 10, NumRows: 1, FieldsData: []*schemapb.FieldData{{
			FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: 32768, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 32768)}},
			}},
		}}}, &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"key"}}}}})).MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(rmq.NewRmqID(int64(tt - 1))).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
}

func TestWALMaterializerUpsertChargesOnlyDeletes(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "autocommit"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			m, tasks, batches := testWALMaterializer(t, 0, 4096)
			upsert := walUpsert(100)
			raw := upsert
			if transaction {
				raw = walTxn(t, upsert)
			}
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			walObserve(m, tracker, raw)
			require.Greater(t, raw.EstimateSize(), 4096)
			deletions, err := messageutil.DeleteRequests(context.Background(), upsert)
			require.NoError(t, err)
			require.Len(t, deletions, 1)
			deletion := deletions[0]
			expected := uint64(proto.Size(deletion.GetPrimaryKeys()) + 8)
			require.Equal(t, expected, m.pendingBytes, "vector bytes belong to L1, not L0 pressure")
			require.Empty(t, *tasks)
			require.Zero(t, tracker.CompletedPoint().TimeTick)
			m.RequestPersistThrough(raw.TimeTick())
			require.Len(t, *tasks, 1)
			require.NoError(t, (*tasks)[0].Execute(context.Background()))
			require.Equal(t, raw.TimeTick(), tracker.CompletedPoint().TimeTick)
			require.Len(t, *batches, 1)
			entry := (*batches)[0].Entries[0]
			require.Equal(t, raw.TimeTick(), entry.GetTimeTick())
			require.Equal(t, int64(-1), entry.GetDelete().GetBlocks()[0].GetPartitionId())
			require.Equal(t, []string{"key"}, entry.GetDelete().GetBlocks()[0].GetPrimaryKeys().GetStrId().GetData())
			require.Zero(t, m.pendingBytes)
		})
	}
}

func TestSummaryReaderMaterializerConsumesUpsert(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 0)
	raw := walTxn(t, walUpsert(100))
	summary := m.reader.(*walsummary.Manager)
	summary.ObserveMessage(context.Background(), raw)
	m.ObserveMessage(raw)
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, raw.TimeTick(), m.MaterializedTimeTick())
	require.Len(t, *batches, 1)
	entry := (*batches)[0].Entries[0]
	require.Equal(t, raw.TimeTick(), entry.GetTimeTick())
	require.Equal(t, []string{"key"}, entry.GetDelete().GetBlocks()[0].GetPrimaryKeys().GetStrId().GetData())
}
