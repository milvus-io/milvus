package streamingcoord

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// NewCataLog creates a new catalog instance
// streamingcoord-meta
// ├── version
// ├── cchannel
// ├── broadcast
// │   ├── task-1
// │   └── task-2
// └── pchannel
//
//	├── pchannel-1
//	└── pchannel-2
//
// └── replicate-configuration
// └── replicating-pchannel
// │   ├── cluster-1-pchannel-1
// │   └── cluster-1-pchannel-2
// │   ├── cluster-2-pchannel-1
// │   └── cluster-2-pchannel-2
func NewCataLog(metaKV kv.MetaKv) metastore.StreamingCoordCataLog {
	return &catalog{
		metaKV: kv.NewReliableWriteMetaKv(metaKV),
	}
}

func NewReplicationCatalog(metaKV kv.MetaKv) metastore.ReplicationCatalog {
	return &catalog{
		metaKV: metaKV,
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
}

// GetCChannel returns the control channel
func (c *catalog) GetCChannel(ctx context.Context) (*streamingpb.CChannelMeta, error) {
	value, err := c.metaKV.Load(ctx, CChannelMetaPrefix)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	info := &streamingpb.CChannelMeta{}
	if err = proto.Unmarshal([]byte(value), info); err != nil {
		return nil, errors.Wrapf(err, "unmarshal cchannel meta failed")
	}
	return info, nil
}

// SaveCChannel saves the control channel
func (c *catalog) SaveCChannel(ctx context.Context, info *streamingpb.CChannelMeta) error {
	v, err := proto.Marshal(info)
	if err != nil {
		return errors.Wrapf(err, "marshal cchannel meta failed")
	}
	return c.metaKV.Save(ctx, CChannelMetaPrefix, string(v))
}

// GetVersion returns the streaming version
func (c *catalog) GetVersion(ctx context.Context) (*streamingpb.StreamingVersion, error) {
	value, err := c.metaKV.Load(ctx, VersionPrefix)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	info := &streamingpb.StreamingVersion{}
	if err = proto.Unmarshal([]byte(value), info); err != nil {
		return nil, errors.Wrapf(err, "unmarshal streaming version failed")
	}
	return info, nil
}

// SaveVersion saves the streaming version
func (c *catalog) SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion) error {
	if version == nil {
		return errors.New("version is nil")
	}
	v, err := proto.Marshal(version)
	if err != nil {
		return errors.Wrapf(err, "marshal streaming version failed")
	}
	return c.metaKV.Save(ctx, VersionPrefix, string(v))
}

// ListPChannels returns all pchannels
func (c *catalog) ListPChannel(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, PChannelMetaPrefix)
	if err != nil {
		return nil, err
	}

	infos := make([]*streamingpb.PChannelMeta, 0, len(values))
	for k, value := range values {
		info := &streamingpb.PChannelMeta{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal pchannel %s failed", keys[k])
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// SavePChannels saves a pchannel
func (c *catalog) SavePChannels(ctx context.Context, infos []*streamingpb.PChannelMeta) error {
	kvs := make(map[string]string, len(infos))
	for _, info := range infos {
		key := buildPChannelInfoPath(info.GetChannel().GetName())
		v, err := proto.Marshal(info)
		if err != nil {
			return errors.Wrapf(err, "marshal pchannel %s failed", info.GetChannel().GetName())
		}
		kvs[key] = string(v)
	}
	return etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, func(partialKvs map[string]string) error {
		return c.metaKV.MultiSave(ctx, partialKvs)
	})
}

func (c *catalog) ListBroadcastTask(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, BroadcastTaskPrefix)
	if err != nil {
		return nil, err
	}
	infos := make([]*streamingpb.BroadcastTask, 0, len(values))
	for k, value := range values {
		info := &streamingpb.BroadcastTask{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal broadcast task %s failed", keys[k])
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (c *catalog) SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask) error {
	key := buildBroadcastTaskPath(broadcastID)
	if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		return c.metaKV.Remove(ctx, key)
	}
	v, err := proto.Marshal(task)
	if err != nil {
		return errors.Wrapf(err, "marshal broadcast task failed")
	}
	return c.metaKV.Save(ctx, key, string(v))
}

// buildPChannelInfoPath builds the path for pchannel info.
func buildPChannelInfoPath(name string) string {
	return PChannelMetaPrefix + name
}

// buildBroadcastTaskPath builds the path for broadcast task.
func buildBroadcastTaskPath(id uint64) string {
	return BroadcastTaskPrefix + strconv.FormatUint(id, 10)
}

func (c *catalog) SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
	v, err := proto.Marshal(config)
	if err != nil {
		return errors.Wrapf(err, "marshal replicate configuration failed")
	}

	kvs := make(map[string]string, len(replicatingTasks)+1)
	kvs[ReplicateConfigurationKey] = string(v)

	for _, task := range replicatingTasks {
		key := buildReplicatePChannelPath(task.GetTargetCluster().GetClusterId(), task.GetSourceChannelName())
		v, err := proto.Marshal(task)
		if err != nil {
			return errors.Wrapf(err, "marshal replicate pchannel meta failed")
		}
		kvs[key] = string(v)
	}
	return etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, func(partialKvs map[string]string) error {
		return c.metaKV.MultiSave(ctx, partialKvs)
	})
}

func (c *catalog) GetReplicateConfiguration(ctx context.Context) (*streamingpb.ReplicateConfigurationMeta, error) {
	key := ReplicateConfigurationKey
	value, err := c.metaKV.Load(ctx, key)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	config := &streamingpb.ReplicateConfigurationMeta{}
	if err = proto.Unmarshal([]byte(value), config); err != nil {
		return nil, errors.Wrapf(err, "unmarshal replicate configuration failed")
	}
	return config, nil
}

func (c *catalog) RemoveReplicatePChannel(ctx context.Context, task *streamingpb.ReplicatePChannelMeta) error {
	key := buildReplicatePChannelPath(task.GetTargetCluster().GetClusterId(), task.GetSourceChannelName())
	return c.metaKV.Remove(ctx, key)
}

func (c *catalog) ListReplicatePChannels(ctx context.Context) ([]*streamingpb.ReplicatePChannelMeta, error) {
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, ReplicatePChannelMetaPrefix)
	if err != nil {
		return nil, err
	}
	infos := make([]*streamingpb.ReplicatePChannelMeta, 0, len(values))
	for k, value := range values {
		info := &streamingpb.ReplicatePChannelMeta{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal replicate pchannel meta %s failed", keys[k])
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func BuildReplicatePChannelMetaKey(meta *streamingpb.ReplicatePChannelMeta) string {
	targetClusterID := meta.GetTargetCluster().GetClusterId()
	sourceChannelName := meta.GetSourceChannelName()
	return buildReplicatePChannelPath(targetClusterID, sourceChannelName)
}

func buildReplicatePChannelPath(targetClusterID, sourceChannelName string) string {
	return fmt.Sprintf("%s%s-%s", ReplicatePChannelMetaPrefix, targetClusterID, sourceChannelName)
}
