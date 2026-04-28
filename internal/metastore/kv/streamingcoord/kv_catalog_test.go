package streamingcoord

import (
	"context"
	"maps"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	canonicalCChannelMetaKeyForTest = "streamingcoord-meta/cchannel"
	legacyCChannelMetaKeyForTest    = canonicalCChannelMetaKeyForTest + "/"
	canonicalVersionKeyForTest      = "streamingcoord-meta/version"
	legacyVersionKeyForTest         = canonicalVersionKeyForTest + "/"
)

func newTestCatalog(t *testing.T) (metastore.StreamingCoordCataLog, map[string]string, *mock_kv.MockMetaKv) {
	kv := mock_kv.NewMockMetaKv(t)
	kvStorage := make(map[string]string)
	kv.EXPECT().Load(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key string) (string, error) {
		value, ok := kvStorage[key]
		if !ok {
			return "", merr.WrapErrIoKeyNotFound(key)
		}
		return value, nil
	}).Maybe()
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, prefix string) ([]string, []string, error) {
		keys := make([]string, 0, len(kvStorage))
		vals := make([]string, 0, len(kvStorage))
		for key, value := range kvStorage {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
				vals = append(vals, value)
			}
		}
		return keys, vals, nil
	}).Maybe()
	kv.EXPECT().MultiLoad(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, keys []string) ([]string, error) {
		values := make([]string, 0, len(keys))
		missing := make([]string, 0)
		for _, key := range keys {
			value, ok := kvStorage[key]
			if !ok {
				missing = append(missing, key)
			}
			values = append(values, value)
		}
		if len(missing) > 0 {
			return values, errors.New("missing keys")
		}
		return values, nil
	}).Maybe()
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, kvs map[string]string) error {
		maps.Copy(kvStorage, kvs)
		return nil
	}).Maybe()
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
		if len(preds) > 0 {
			return merr.WrapErrServiceUnavailable("predicates not supported")
		}
		for _, key := range removals {
			delete(kvStorage, key)
		}
		maps.Copy(kvStorage, saves)
		return nil
	}).Maybe()
	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key, value string) error {
		kvStorage[key] = value
		return nil
	}).Maybe()
	kv.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key string) error {
		delete(kvStorage, key)
		return nil
	}).Maybe()
	return NewCataLog(kv), kvStorage, kv
}

func TestCatalogMetaKeys(t *testing.T) {
	assert.Equal(t, canonicalCChannelMetaKeyForTest, CChannelMetaKey)
	assert.Equal(t, canonicalVersionKeyForTest, VersionKey)
}

func TestCatalog(t *testing.T) {
	catalog, _, kv := newTestCatalog(t)

	metas, err := catalog.ListPChannel(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, metas)

	// Version test
	err = catalog.SaveVersion(context.Background(), &streamingpb.StreamingVersion{
		Version: 1,
	})
	assert.NoError(t, err)

	v, err := catalog.GetVersion(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, v.Version, int64(1))

	// CChannel test
	err = catalog.SaveCChannel(context.Background(), &streamingpb.CChannelMeta{
		Pchannel: "test",
	})
	assert.NoError(t, err)

	assignments, err := catalog.GetCChannel(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, assignments.Pchannel, "test")

	// PChannel test
	err = catalog.SavePChannels(context.Background(), []*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		},
		{
			Channel: &streamingpb.PChannelInfo{Name: "test2", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		},
	})
	assert.NoError(t, err)

	metas, err = catalog.ListPChannel(context.Background())
	assert.NoError(t, err)
	assert.Len(t, metas, 2)

	// BroadcastTask test
	err = catalog.SaveBroadcastTask(context.Background(), 1, &streamingpb.BroadcastTask{
		State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
	})
	assert.NoError(t, err)
	err = catalog.SaveBroadcastTask(context.Background(), 2, &streamingpb.BroadcastTask{
		State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
	})
	assert.NoError(t, err)

	tasks, err := catalog.ListBroadcastTask(context.Background())
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
	for _, task := range tasks {
		assert.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, task.State)
	}

	err = catalog.SaveBroadcastTask(context.Background(), 1, &streamingpb.BroadcastTask{
		State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE,
	})
	assert.NoError(t, err)
	tasks, err = catalog.ListBroadcastTask(context.Background())
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	for _, task := range tasks {
		assert.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, task.State)
	}

	// error path.
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, errors.New("load error"))
	metas, err = catalog.ListPChannel(context.Background())
	assert.Error(t, err)
	assert.Nil(t, metas)

	tasks, err = catalog.ListBroadcastTask(context.Background())
	assert.Error(t, err)
	assert.Nil(t, tasks)
}

func TestCatalog_CChannelMetaKeyCompatibility(t *testing.T) {
	ctx := context.Background()

	t.Run("reads and repairs legacy key", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		legacyValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "legacy-channel"})
		require.NoError(t, err)
		kvStorage[legacyCChannelMetaKeyForTest] = string(legacyValue)

		cchannel, err := catalog.GetCChannel(ctx)

		require.NoError(t, err)
		require.NotNil(t, cchannel)
		assert.Equal(t, "legacy-channel", cchannel.GetPchannel())
		assert.Contains(t, kvStorage, canonicalCChannelMetaKeyForTest)
		assert.NotContains(t, kvStorage, legacyCChannelMetaKeyForTest)
	})

	t.Run("prefers canonical key", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		canonicalValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "canonical-channel"})
		require.NoError(t, err)
		legacyValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "legacy-channel"})
		require.NoError(t, err)
		kvStorage[canonicalCChannelMetaKeyForTest] = string(canonicalValue)
		kvStorage[legacyCChannelMetaKeyForTest] = string(legacyValue)

		cchannel, err := catalog.GetCChannel(ctx)

		require.NoError(t, err)
		require.NotNil(t, cchannel)
		assert.Equal(t, "canonical-channel", cchannel.GetPchannel())
		assert.Contains(t, kvStorage, canonicalCChannelMetaKeyForTest)
		assert.NotContains(t, kvStorage, legacyCChannelMetaKeyForTest)
	})

	t.Run("does not repair when only canonical key exists", func(t *testing.T) {
		catalog, kvStorage, kv := newTestCatalog(t)
		canonicalValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "canonical-channel"})
		require.NoError(t, err)
		kvStorage[canonicalCChannelMetaKeyForTest] = string(canonicalValue)

		cchannel, err := catalog.GetCChannel(ctx)

		require.NoError(t, err)
		require.NotNil(t, cchannel)
		assert.Equal(t, "canonical-channel", cchannel.GetPchannel())
		kv.AssertNotCalled(t, "Remove", mock.Anything, legacyCChannelMetaKeyForTest)
		kv.AssertNotCalled(t, "MultiSaveAndRemove", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("ignores child keys", func(t *testing.T) {
		catalog, kvStorage, kv := newTestCatalog(t)
		childValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "child-channel"})
		require.NoError(t, err)
		kvStorage[canonicalCChannelMetaKeyForTest+"/child"] = string(childValue)

		cchannel, err := catalog.GetCChannel(ctx)

		require.NoError(t, err)
		assert.Nil(t, cchannel)
		kv.AssertNotCalled(t, "Remove", mock.Anything, legacyCChannelMetaKeyForTest)
		kv.AssertNotCalled(t, "MultiSaveAndRemove", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("save writes canonical key and removes legacy key", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		legacyValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "legacy-channel"})
		require.NoError(t, err)
		kvStorage[legacyCChannelMetaKeyForTest] = string(legacyValue)

		err = catalog.SaveCChannel(ctx, &streamingpb.CChannelMeta{Pchannel: "saved-channel"})

		require.NoError(t, err)
		assert.Contains(t, kvStorage, canonicalCChannelMetaKeyForTest)
		assert.NotContains(t, kvStorage, legacyCChannelMetaKeyForTest)
		cchannel, err := catalog.GetCChannel(ctx)
		require.NoError(t, err)
		require.NotNil(t, cchannel)
		assert.Equal(t, "saved-channel", cchannel.GetPchannel())
	})

	t.Run("keeps legacy key when read repair value is invalid", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		kvStorage[legacyCChannelMetaKeyForTest] = "invalid"

		cchannel, err := catalog.GetCChannel(ctx)

		assert.Error(t, err)
		assert.Nil(t, cchannel)
		assert.NotContains(t, kvStorage, canonicalCChannelMetaKeyForTest)
		assert.Contains(t, kvStorage, legacyCChannelMetaKeyForTest)
	})

	t.Run("does not fall back when canonical value is invalid", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		legacyValue, err := proto.Marshal(&streamingpb.CChannelMeta{Pchannel: "legacy-channel"})
		require.NoError(t, err)
		kvStorage[canonicalCChannelMetaKeyForTest] = "invalid"
		kvStorage[legacyCChannelMetaKeyForTest] = string(legacyValue)

		cchannel, err := catalog.GetCChannel(ctx)

		assert.Error(t, err)
		assert.Nil(t, cchannel)
		assert.Contains(t, kvStorage, canonicalCChannelMetaKeyForTest)
		assert.Contains(t, kvStorage, legacyCChannelMetaKeyForTest)
	})
}

func TestCatalog_VersionMetaKeyCompatibility(t *testing.T) {
	ctx := context.Background()

	t.Run("reads and repairs legacy key", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		legacyValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 1})
		require.NoError(t, err)
		kvStorage[legacyVersionKeyForTest] = string(legacyValue)

		version, err := catalog.GetVersion(ctx)

		require.NoError(t, err)
		require.NotNil(t, version)
		assert.Equal(t, int64(1), version.GetVersion())
		assert.Contains(t, kvStorage, canonicalVersionKeyForTest)
		assert.NotContains(t, kvStorage, legacyVersionKeyForTest)
	})

	t.Run("prefers canonical key", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		canonicalValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 2})
		require.NoError(t, err)
		legacyValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 1})
		require.NoError(t, err)
		kvStorage[canonicalVersionKeyForTest] = string(canonicalValue)
		kvStorage[legacyVersionKeyForTest] = string(legacyValue)

		version, err := catalog.GetVersion(ctx)

		require.NoError(t, err)
		require.NotNil(t, version)
		assert.Equal(t, int64(2), version.GetVersion())
		assert.Contains(t, kvStorage, canonicalVersionKeyForTest)
		assert.NotContains(t, kvStorage, legacyVersionKeyForTest)
	})

	t.Run("does not repair when only canonical key exists", func(t *testing.T) {
		catalog, kvStorage, kv := newTestCatalog(t)
		canonicalValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 2})
		require.NoError(t, err)
		kvStorage[canonicalVersionKeyForTest] = string(canonicalValue)

		version, err := catalog.GetVersion(ctx)

		require.NoError(t, err)
		require.NotNil(t, version)
		assert.Equal(t, int64(2), version.GetVersion())
		kv.AssertNotCalled(t, "Remove", mock.Anything, legacyVersionKeyForTest)
		kv.AssertNotCalled(t, "MultiSaveAndRemove", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("reads empty canonical value", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		canonicalValue, err := proto.Marshal(&streamingpb.StreamingVersion{})
		require.NoError(t, err)
		kvStorage[canonicalVersionKeyForTest] = string(canonicalValue)

		version, err := catalog.GetVersion(ctx)

		require.NoError(t, err)
		require.NotNil(t, version)
		assert.Equal(t, int64(0), version.GetVersion())
	})

	t.Run("ignores child keys", func(t *testing.T) {
		catalog, kvStorage, kv := newTestCatalog(t)
		childValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 3})
		require.NoError(t, err)
		kvStorage[canonicalVersionKeyForTest+"/child"] = string(childValue)

		version, err := catalog.GetVersion(ctx)

		require.NoError(t, err)
		assert.Nil(t, version)
		kv.AssertNotCalled(t, "Remove", mock.Anything, legacyVersionKeyForTest)
		kv.AssertNotCalled(t, "MultiSaveAndRemove", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("save writes canonical key and removes legacy key", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		legacyValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 1})
		require.NoError(t, err)
		kvStorage[legacyVersionKeyForTest] = string(legacyValue)

		err = catalog.SaveVersion(ctx, &streamingpb.StreamingVersion{Version: 2})

		require.NoError(t, err)
		assert.Contains(t, kvStorage, canonicalVersionKeyForTest)
		assert.NotContains(t, kvStorage, legacyVersionKeyForTest)
		version, err := catalog.GetVersion(ctx)
		require.NoError(t, err)
		require.NotNil(t, version)
		assert.Equal(t, int64(2), version.GetVersion())
	})

	t.Run("keeps legacy key when read repair value is invalid", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		kvStorage[legacyVersionKeyForTest] = "invalid"

		version, err := catalog.GetVersion(ctx)

		assert.Error(t, err)
		assert.Nil(t, version)
		assert.NotContains(t, kvStorage, canonicalVersionKeyForTest)
		assert.Contains(t, kvStorage, legacyVersionKeyForTest)
	})

	t.Run("does not fall back when canonical value is invalid", func(t *testing.T) {
		catalog, kvStorage, _ := newTestCatalog(t)
		legacyValue, err := proto.Marshal(&streamingpb.StreamingVersion{Version: 1})
		require.NoError(t, err)
		kvStorage[canonicalVersionKeyForTest] = "invalid"
		kvStorage[legacyVersionKeyForTest] = string(legacyValue)

		version, err := catalog.GetVersion(ctx)

		assert.Error(t, err)
		assert.Nil(t, version)
		assert.Contains(t, kvStorage, canonicalVersionKeyForTest)
		assert.Contains(t, kvStorage, legacyVersionKeyForTest)
	})
}

func TestCatalog_ReplicationCatalog(t *testing.T) {
	catalog, _, _ := newTestCatalog(t)

	// ReplicateConfiguration test
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				Pchannels: []string{"source-channel-1", "source-channel-2"},
			},
			{
				ClusterId: "target-cluster-a",
				Pchannels: []string{"target-channel-a-1", "target-channel-a-2"},
			},
			{
				ClusterId: "target-cluster-b",
				Pchannels: []string{"target-channel-b-1", "target-channel-b-2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-a",
			},
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-b",
			},
		},
	}
	err := catalog.SaveReplicateConfiguration(context.Background(), &streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: config}, nil)
	assert.NoError(t, err)

	cfg, err := catalog.GetReplicateConfiguration(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, cfg.ReplicateConfiguration.GetClusters()[0].GetClusterId(), "source-cluster")
	assert.Equal(t, cfg.ReplicateConfiguration.GetClusters()[1].GetClusterId(), "target-cluster-a")
	assert.Equal(t, cfg.ReplicateConfiguration.GetClusters()[2].GetClusterId(), "target-cluster-b")
	assert.Equal(t, cfg.ReplicateConfiguration.GetCrossClusterTopology()[0].GetSourceClusterId(), "source-cluster")
	assert.Equal(t, cfg.ReplicateConfiguration.GetCrossClusterTopology()[0].GetTargetClusterId(), "target-cluster-a")
	assert.Equal(t, cfg.ReplicateConfiguration.GetCrossClusterTopology()[1].GetSourceClusterId(), "source-cluster")
	assert.Equal(t, cfg.ReplicateConfiguration.GetCrossClusterTopology()[1].GetTargetClusterId(), "target-cluster-b")

	// ReplicatePChannel test
	err = catalog.SaveReplicateConfiguration(context.Background(),
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: config},
		[]*streamingpb.ReplicatePChannelMeta{
			{
				SourceChannelName: "source-channel-1",
				TargetChannelName: "target-channel-1",
				TargetCluster:     &commonpb.MilvusCluster{ClusterId: "target-cluster"},
			},
			{
				SourceChannelName: "source-channel-2",
				TargetChannelName: "target-channel-2",
				TargetCluster:     &commonpb.MilvusCluster{ClusterId: "target-cluster"},
			},
		})
	assert.NoError(t, err)
}
