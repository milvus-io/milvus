package querycoordv2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestQueryViewUpdateLoadConfig(t *testing.T) {
	s, _ := newHandoffServer(t)
	s.meta = &meta.Meta{ReplicaManager: &meta.ReplicaManager{}, ResourceManager: &meta.ResourceManager{}}
	s.qviewsRuntime.loadManager = loadmgr.NewCollectionLoadManager(s.qviewsRuntime.loadConfigStore, nil, s.meta)
	store := s.qviewsRuntime.loadConfigStore
	cfg := store.GetConfig(1).Clone()
	cfg.Replicas = []*loadmgr.ReplicaAssignment{{ReplicaID: 11, ResourceGroup: "a"}, {ReplicaID: 12, ResourceGroup: "a"}, {ReplicaID: 13, ResourceGroup: "b"}}
	cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 100}}
	cfg.RequiredFields = []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}}
	cfg.UserSpecifiedReplicaMode = true
	require.NoError(t, store.Put(t.Context(), cfg))
	for _, p := range []*mockey.Mocker{
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build(),
		mockey.Mock((*meta.ResourceManager).ContainResourceGroup).Return(true).Build(),
	} {
		t.Cleanup(func() { p.UnPatch() })
	}
	broadcast := mockey.Mock(stubBroadcaster.Broadcast).Return(nil, merr.WrapErrServiceInternalMsg("unexpected broadcast")).Build()
	t.Cleanup(func() { require.Zero(t, broadcast.Times()); broadcast.UnPatch() })
	require.NoError(t, s.updateLoadConfig(t.Context(), []int64{1}, 3, []string{"a", "b", "b"}))
	require.Equal(t, cfg.LoadFields, store.GetConfig(1).LoadFields)
	require.Equal(t, cfg.RequiredFields, store.GetConfig(1).RequiredFields)
	require.Equal(t, cfg.PartitionIDs, store.GetConfig(1).PartitionIDs)
	require.True(t, store.GetConfig(1).UserSpecifiedReplicaMode)
	require.Equal(t, "b", store.GetConfig(1).Replicas[1].ResourceGroup)
	require.NoError(t, s.updateLoadConfig(t.Context(), []int64{1}, 3, []string{"b", "a", "b"}))

	params := paramtable.Get()
	for key, value := range map[string]string{
		params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:                "3",
		params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key:               "a,a,b",
		params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key: "false",
	} {
		old := params.GetWithDefault(key, "")
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
	}
	watcher := &LoadConfigWatcher{s: s, notifier: syncutil.NewAsyncTaskNotifier[struct{}]()}
	require.NoError(t, watcher.applyLoadConfigChanges())
	require.Equal(t, "b", store.GetConfig(1).Replicas[1].ResourceGroup, "user placement is protected")
	require.NoError(t, params.Save(params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "true"))
	require.NoError(t, watcher.applyLoadConfigChanges())
	require.True(t, store.GetConfig(1).UserSpecifiedReplicaMode, "force does not erase user intent")
	var groups []string
	for _, replica := range store.GetConfig(1).Replicas {
		groups = append(groups, replica.ResourceGroup)
	}
	require.ElementsMatch(t, []string{"a", "a", "b"}, groups)
	version := store.GetConfigVersion(1)
	require.NoError(t, watcher.applyLoadConfigChanges())
	require.Equal(t, version, store.GetConfigVersion(1), "unchanged config is not republished")
	require.NoError(t, store.Remove(t.Context(), 1))
	require.NoError(t, s.updateLoadConfig(t.Context(), []int64{1}, 3, []string{"a"}))
}

func TestQueryViewLoadConfigWatcherLifecycle(t *testing.T) {
	s, _ := newHandoffServer(t)
	entered := make(chan struct{}, 1)
	patch := mockey.Mock((*Server).updateCollectionLoadConfig).To(func(_ *Server, ctx context.Context, id int64, _ int32, _ []string, defaults bool) error {
		require.EqualValues(t, 1, id)
		require.True(t, defaults)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}).Build()
	defer patch.UnPatch()
	s.watchLoadConfigChanges()
	select {
	case <-entered: // Startup reconciles recovered configurations without an event.
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not reconcile at startup")
	}
	require.Len(t, s.loadConfigWatcher.handlers, 3)
	// Waiting for a DDL guard must not block config delivery. Notifications coalesce.
	for range 1000 {
		s.loadConfigWatcher.Trigger()
	}
	stopped := make(chan struct{})
	go func() { s.loadConfigWatcher.Close(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not cancel outstanding work")
	}
}

func TestQueryViewReplicaConfigDirectApply(t *testing.T) {
	s, _ := newHandoffServer(t)
	s.meta = &meta.Meta{ReplicaManager: &meta.ReplicaManager{}, ResourceManager: &meta.ResourceManager{}}
	s.qviewsRuntime.loadManager = loadmgr.NewCollectionLoadManager(s.qviewsRuntime.loadConfigStore, nil, s.meta)
	store := s.qviewsRuntime.loadConfigStore
	cfg := store.GetConfig(1).Clone()
	cfg.Replicas = []*loadmgr.ReplicaAssignment{{ReplicaID: 11, ResourceGroup: "a"}}
	cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 100}}
	cfg.RequiredFields = []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}}
	cfg.UserSpecifiedReplicaMode = true
	require.NoError(t, store.Put(t.Context(), cfg))
	allocated := int64(11)
	var allocationErr error
	for _, p := range []*mockey.Mocker{
		mockey.Mock((*meta.ResourceManager).ContainResourceGroup).Return(true).Build(),
		mockey.Mock((*meta.ReplicaManager).AllocateReplicaID).To(func(*meta.ReplicaManager, context.Context) (int64, error) {
			if allocationErr != nil {
				return 0, allocationErr
			}
			allocated++
			return allocated, nil
		}).Build(),
	} {
		t.Cleanup(func() { p.UnPatch() })
	}
	allocationErr = context.DeadlineExceeded
	err := s.qviewsRuntime.loadManager.UpdateReplicaConfig(t.Context(), 1, 3, []string{"a", "b", "b"})
	require.ErrorIs(t, err, allocationErr)
	require.Equal(t, cfg, store.GetConfig(1), "construction failures do not change desired state")
	allocationErr = nil
	err = s.qviewsRuntime.loadManager.UpdateReplicaConfig(t.Context(), 1, 3, []string{"a", "b", "b"})
	require.NoError(t, err)
	next := store.GetConfig(1)
	require.Equal(t, cfg.PartitionIDs, next.PartitionIDs)
	require.Equal(t, cfg.LoadFields, next.LoadFields)
	require.Equal(t, cfg.RequiredFields, next.RequiredFields)
	require.True(t, next.UserSpecifiedReplicaMode)
	require.EqualValues(t, 11, next.Replicas[0].ReplicaID)
	require.EqualValues(t, 13, allocated)
	err = s.qviewsRuntime.loadManager.UpdateReplicaConfig(t.Context(), 1, 3, []string{"b", "a", "b"})
	require.NoError(t, err)
	require.EqualValues(t, 13, allocated)
}

func TestQueryViewLoadConfigWatcherRechecksAfterLock(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprintf("release=%v", release), func(t *testing.T) {
			s, _ := newHandoffServer(t)
			store := s.qviewsRuntime.loadConfigStore
			params := paramtable.Get()
			for key, value := range map[string]string{
				params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:  "2",
				params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key: "a",
			} {
				old := params.GetWithDefault(key, "")
				require.NoError(t, params.Save(key, value))
				t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
			}
			guard := mockey.Mock((*Server).startBroadcastWithCollectionIDLock).To(func(_ *Server, ctx context.Context, _ int64) (broadcaster.BroadcastAPI, error) {
				// Simulate a release or explicit load winning the lock before the
				// watcher. Its earlier snapshot must not overwrite either one.
				if release {
					require.NoError(t, store.Remove(ctx, 1))
				} else {
					cfg := store.GetConfig(1).Clone()
					cfg.UserSpecifiedReplicaMode = true
					require.NoError(t, store.Put(ctx, cfg))
				}
				return stubBroadcaster{}, nil
			}).Build()
			defer guard.UnPatch()
			watcher := &LoadConfigWatcher{s: s, notifier: syncutil.NewAsyncTaskNotifier[struct{}]()}
			require.NoError(t, watcher.applyLoadConfigChanges())
			require.Equal(t, 1, guard.Times())
			if release {
				require.False(t, store.Contains(1))
			} else {
				require.True(t, store.GetConfig(1).UserSpecifiedReplicaMode)
				require.Len(t, store.GetConfig(1).Replicas, 1)
			}
		})
	}
}
