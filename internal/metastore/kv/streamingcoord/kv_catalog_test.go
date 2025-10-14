package streamingcoord

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestCatalog(t *testing.T) {
	kv := mock_kv.NewMockMetaKv(t)

	kvStorage := make(map[string]string)
	kv.EXPECT().Load(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (string, error) {
		return kvStorage[s], nil
	})
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) ([]string, []string, error) {
		keys := make([]string, 0, len(kvStorage))
		vals := make([]string, 0, len(kvStorage))
		for k, v := range kvStorage {
			if strings.HasPrefix(k, s) {
				keys = append(keys, k)
				vals = append(vals, v)
			}
		}
		return keys, vals, nil
	})
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, kvs map[string]string) error {
		for k, v := range kvs {
			kvStorage[k] = v
		}
		return nil
	})
	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key, value string) error {
		kvStorage[key] = value
		return nil
	})
	kv.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key string) error {
		delete(kvStorage, key)
		return nil
	})

	catalog := NewCataLog(kv)
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

func TestCatalog_ReplicationCatalog(t *testing.T) {
	kv := mock_kv.NewMockMetaKv(t)
	kvStorage := make(map[string]string)
	kv.EXPECT().Load(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (string, error) {
		return kvStorage[s], nil
	})
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) ([]string, []string, error) {
		keys := make([]string, 0, len(kvStorage))
		vals := make([]string, 0, len(kvStorage))
		for k, v := range kvStorage {
			if strings.HasPrefix(k, s) {
				keys = append(keys, k)
				vals = append(vals, v)
			}
		}
		return keys, vals, nil
	})
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, kvs map[string]string) error {
		for k, v := range kvs {
			kvStorage[k] = v
		}
		return nil
	})
	kv.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, key string) error {
		delete(kvStorage, key)
		return nil
	})

	catalog := NewCataLog(kv)

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

	infos, err := catalog.ListReplicatePChannels(context.Background())
	assert.NoError(t, err)
	assert.Len(t, infos, 2)
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].GetTargetChannelName() < infos[j].GetTargetChannelName()
	})
	assert.Equal(t, infos[0].GetSourceChannelName(), "source-channel-1")
	assert.Equal(t, infos[0].GetTargetChannelName(), "target-channel-1")
	assert.Equal(t, infos[0].GetTargetCluster().GetClusterId(), "target-cluster")
	assert.Equal(t, infos[1].GetSourceChannelName(), "source-channel-2")
	assert.Equal(t, infos[1].GetTargetChannelName(), "target-channel-2")
	assert.Equal(t, infos[1].GetTargetCluster().GetClusterId(), "target-cluster")

	err = catalog.RemoveReplicatePChannel(context.Background(), &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "source-channel-1",
		TargetChannelName: "target-channel-1",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "target-cluster"},
	})
	assert.NoError(t, err)

	infos, err = catalog.ListReplicatePChannels(context.Background())
	assert.NoError(t, err)
	assert.Len(t, infos, 1)
	assert.Equal(t, infos[0].GetSourceChannelName(), "source-channel-2")
	assert.Equal(t, infos[0].GetTargetChannelName(), "target-channel-2")
	assert.Equal(t, infos[0].GetTargetCluster().GetClusterId(), "target-cluster")

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", merr.ErrIoKeyNotFound)

	cfg, err = catalog.GetReplicateConfiguration(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, cfg)
}
