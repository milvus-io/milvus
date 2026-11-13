package partialupdate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestPartialUpdateRecoveryReadsNewFormatWriteSets(t *testing.T) {
	for _, kind := range []string{"insert", "upsert", "cas-upsert"} {
		t.Run(kind, func(t *testing.T) {
			rows := &messagespb.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 10}}}}, RowIdsEncoding: &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: &messagespb.DeltaEncodedRowIDs{Base: 100, Deltas: []int64{1}}}}
			partitions := []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}}
			schemaVersion := int32(1)
			var body message.MutableMessage
			expected := []int64{10, 10}
			if kind == "insert" {
				body = message.NewInsertMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.InsertMessageHeaderV2{CollectionId: 1, SchemaVersion: &schemaVersion, Partitions: partitions}).WithBody(&messagespb.InsertMessageBody{Rows: rows}).MustBuildMutable()
			} else {
				upsert := &messagespb.UpsertMessageBody{Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{
					{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{20, 30}}}}}},
					{Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 20}, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}},
				}}
				builder := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &schemaVersion, Partitions: partitions, DeleteRows: 4})
				if kind == "cas-upsert" {
					upsert.PartialUpdateCas = validCASMeta(5, 1)
					builder.WithProperty("_puc", "")
				}
				body = builder.WithBody(upsert).MustBuildMutable()
				expected = []int64{10, 10, 20, 30, 10, 10}
			}
			env := newPartialUpdateChainTestEnvWithBuilders(t, recoveredRowLockBuilders(t, body))
			state := env.partial.state.getTxn(1)
			require.NotNil(t, state)
			require.True(t, state.recovered)
			require.True(t, state.observedBegin, "a decoded new-format write set must not force ordinary Commit to expire")
			require.Equal(t, expected, state.pks.int64Values)
			if kind == "cas-upsert" {
				require.Equal(t, uint64(5), state.meta.ReadTs)
				require.Equal(t, int64(1), state.collectionID)
				require.Equal(t, int32(1), state.schemaVersion)
				appended := false
				_, err := env.append(newCASCommitTxnMessage(t, "v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
					appended = true
					return env.nextMessageID(), nil
				})
				requirePartialUpdateRetryable(t, err)
				require.False(t, appended, "recovered local CAS still requires the existing full retry")
				return
			}
			readTS := env.allocateReadTS(t)
			_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				for _, key := range expected {
					require.NoError(t, testPKSharedError(env.partial.state.rowLocks, "v1", key))
				}
				return env.nextMessageID(), nil
			})
			require.NoError(t, err)
			for _, key := range expected {
				require.True(t, env.hasPKVersionAfter("v1", key, readTS))
			}
			require.Nil(t, env.partial.state.getTxn(1))
		})
	}
}
