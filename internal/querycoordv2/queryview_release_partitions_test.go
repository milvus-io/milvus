package querycoordv2

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReleasePartitionsWaitsForHandoff(t *testing.T) {
	for _, scenario := range []string{"partial", "repeat", "unchanged message", "cancel", "broadcast error"} {
		t.Run(scenario, func(t *testing.T) {
			s, up := newHandoffServer(t)
			partitionPatch := mockey.Mock(catalogkv.Catalog.ReleasePartition).Return(nil).Build()
			t.Cleanup(func() { partitionPatch.UnPatch() })
			store := s.qviewsRuntime.loadConfigStore
			cfg := store.GetConfig(1).Clone()
			cfg.PartitionIDs = []int64{10, 20}
			require.NoError(t, store.Put(t.Context(), cfg))
			up("v0", 1)
			up("v1", 1)
			cfg = cfg.Clone()
			cfg.PartitionIDs = []int64{10}
			if scenario == "repeat" || scenario == "unchanged message" {
				require.NoError(t, store.Put(t.Context(), cfg))
			}
			var closed atomic.Bool
			patches := []*mockey.Mocker{
				mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(releasePartitionsBroadcaster{}, nil).Build(),
				mockey.Mock(releasePartitionsBroadcaster.Close).To(func(releasePartitionsBroadcaster) { closed.Store(true) }).Build(),
				mockey.Mock(job.GenerateAlterLoadConfigMessage).To(func(_ context.Context, req *job.AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
					require.Equal(t, []int64{10}, req.Expected.ExpectedPartitionIDs)
					if scenario == "unchanged message" {
						return nil, nil
					}
					return message.NewAlterLoadConfigMessageBuilderV2().WithHeader(&message.AlterLoadConfigMessageHeader{CollectionId: 1}).WithBody(&message.AlterLoadConfigMessageBody{}).WithControlChannelBroadcast().MustBuildBroadcast(), nil
				}).Build(),
				mockey.Mock(releasePartitionsBroadcaster.Broadcast).To(func(releasePartitionsBroadcaster, context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
					if scenario == "broadcast error" {
						return nil, merr.ErrServiceUnavailable
					}
					return nil, store.Put(t.Context(), cfg)
				}).Build(),
			}
			for _, patch := range patches {
				t.Cleanup(func() { patch.UnPatch() })
			}
			// Force the generator's no-message path while the store already has the
			// desired configuration, as can happen on an idempotent admission replay.
			if scenario == "unchanged message" {
				old := cfg.Clone()
				old.PartitionIDs = []int64{10, 20}
				patch := mockey.Mock((*Server).getCurrentLoadConfig).Return(qviewsCurrentLoadConfig(old)).Build()
				t.Cleanup(func() { patch.UnPatch() })
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.broadcastAlterLoadConfigCollectionV2ForReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{CollectionID: 1, PartitionIDs: []int64{20}})
				done <- err
			}()
			if scenario == "broadcast error" {
				require.ErrorIs(t, <-done, merr.ErrServiceUnavailable)
				return
			}
			assertReadinessBlocked(t, done)
			require.False(t, closed.Load(), "handoff must retain collection serialization")
			up("v0", 1)
			assertReadinessBlocked(t, done)
			if scenario == "cancel" {
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
			} else {
				up("v1", 1)
				require.NoError(t, <-done)
			}
			require.True(t, closed.Load())
		})
	}
}

func TestReleasePartitionsRepeatedFullReleaseWaits(t *testing.T) {
	s := newReleaseTestServer(t)
	manager := s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	s.broker = &meta.CoordinatorBroker{}
	patches := []*mockey.Mocker{
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build(),
		mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Build(),
		mockey.Mock((*Server).getCurrentLoadConfig).Return(job.CurrentLoadConfig{}).Build(),
	}
	for _, patch := range patches {
		t.Cleanup(func() { patch.UnPatch() })
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.broadcastAlterLoadConfigCollectionV2ForReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{CollectionID: 100, PartitionIDs: []int64{20}})
		done <- err
	}()
	assertReadinessBlocked(t, done)
	require.NoError(t, manager.RequestRelease(ctx))
	require.NoError(t, <-done)
}

type releasePartitionsBroadcaster struct{}

func (releasePartitionsBroadcaster) Broadcast(context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	panic("unpatched")
}
func (releasePartitionsBroadcaster) Close() { panic("unpatched") }
