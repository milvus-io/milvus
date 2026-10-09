package streamingnode

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Interrupt every transaction boundary, then reopen the catalog as a new owner.
// Even a single channel's schemas can exceed the transaction limit.
func TestRecoverySnapshotSchemaPublication(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, limit := range []int{1, 2, 128} {
			batches := 1
			if limit < 5 { // Two schemas, a base, a segment, and the global checkpoint.
				batches = (4+limit-1)/limit + 1
			}
			for completed := 0; completed <= batches; completed++ {
				t.Run(fmt.Sprintf("existing=%t/limit=%d/completed=%d", existing, limit, completed), func(t *testing.T) {
					ctx := context.Background()
					client, _ := kvfactory.GetEtcdAndPath()
					store := etcdkv.NewEtcdKV(client, "schema-publication-"+uuid.NewString())
					t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(ctx, "")) })
					catalog := NewCataLog(store)
					cp := &streamingpb.WALCheckpoint{TimeTick: 100, Term: 1}
					require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{ConsumeCheckpoint: cp}))
					old := &streamingpb.VChannelMeta{
						Vchannel: "v1", CheckpointTimeTick: 100,
						State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
						CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1, Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
							CheckpointTimeTick: 100, Schema: &schemapb.CollectionSchema{Version: 1},
							State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						}}},
					}
					if existing {
						require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
							ConsumeCheckpoint: cp, VChannels: map[string]*streamingpb.VChannelMeta{"v1": old},
						}))
					}
					next := proto.Clone(old).(*streamingpb.VChannelMeta)
					next.CheckpointTimeTick = 200
					next.CollectionInfo.Schemas = append(next.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
						CheckpointTimeTick: 200, Schema: &schemapb.CollectionSchema{Version: 2},
						State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					})
					snapshot := &metastore.WALRecoverySnapshot{
						ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 200, Term: 1},
						VChannels:         map[string]*streamingpb.VChannelMeta{"v1": next},
						SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{1: {
							SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 200,
						}},
					}
					limitPatch := mockey.Mock(mockey.GetMethod(store, "MaxTxnOps")).Return(limit).Build()
					t.Cleanup(func() { limitPatch.UnPatch() })
					calls := 0
					failure := merr.WrapErrIoFailedReason("interrupted snapshot")
					var write func(*recoverySnapshotKV, context.Context, map[string]string, []string, ...predicates.Predicate) error
					patch := mockey.Mock((*recoverySnapshotKV).MultiSaveAndRemove).Origin(&write).To(
						func(k *recoverySnapshotKV, ctx context.Context, saves map[string]string, removes []string, preds ...predicates.Predicate) error {
							calls++
							if calls > completed {
								return failure
							}
							return write(k, ctx, saves, removes, preds...)
						}).Build()
					t.Cleanup(func() { patch.UnPatch() })
					err := catalog.SaveRecoverySnapshot(ctx, "p1", snapshot)
					if completed < batches {
						require.ErrorIs(t, err, failure)
					} else {
						require.NoError(t, err)
					}
					patch.UnPatch()

					// Recovery claims ownership before loading partially published components.
					reopened := NewCataLog(store)
					recoveredCP, err := reopened.GetConsumeCheckpoint(ctx, "p1")
					require.NoError(t, err)
					if completed < batches {
						require.Equal(t, uint64(100), recoveredCP.TimeTick)
					} else {
						require.Equal(t, uint64(200), recoveredCP.TimeTick)
					}
					recoveredCP.Term = 2
					require.NoError(t, reopened.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{ConsumeCheckpoint: recoveredCP}))
					channels, err := reopened.ListVChannel(ctx, "p1")
					require.NoError(t, err)
					basePublished := completed == batches || (limit < 5 && completed*limit >= 3)
					if basePublished {
						require.Len(t, channels, 1)
						require.True(t, proto.Equal(next, channels[0]), "published base must have all its schemas")
					} else if existing {
						require.Len(t, channels, 1)
						require.True(t, proto.Equal(old, channels[0]), "future schemas must not leak into the old base")
					} else {
						require.Empty(t, channels, "orphan schemas do not publish a channel")
					}
					segments, err := reopened.ListSegmentAssignment(ctx, "p1")
					require.NoError(t, err)
					if !basePublished {
						require.Empty(t, segments, "segments cannot precede their base and schemas")
					}

					// Replayed state can overwrite uncommitted schemas and publish normally.
					snapshot.ConsumeCheckpoint.Term = 2
					require.NoError(t, reopened.SaveRecoverySnapshot(ctx, "p1", snapshot))
					channels, err = reopened.ListVChannel(ctx, "p1")
					require.NoError(t, err)
					require.Len(t, channels, 1)
					require.True(t, proto.Equal(next, channels[0]))
				})
			}
		}
	}
}

func TestCatalogRejectsBaseWithOnlyFutureSchemas(t *testing.T) {
	catalog := NewCataLog(newRootedMemoryKV("schema-visibility"))
	meta := &streamingpb.VChannelMeta{
		Vchannel: "v1", CheckpointTimeTick: 100,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
			CheckpointTimeTick: 200, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}}},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{"v1": meta},
	}))
	_, err := catalog.ListVChannel(context.Background(), "p1")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "missing schemas")
}
