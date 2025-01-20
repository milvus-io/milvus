package streamingcoord

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

// NewCataLog creates a new catalog instance
// streamingcoord-meta
// ├── broadcast
// │   ├── task-1
// │   └── task-2
// └── pchannel
//
//	├── pchannel-1
//	└── pchannel-2
func NewCataLog(metaKV kv.MetaKv) metastore.StreamingCoordCataLog {
	return &catalog{
		metaKV: metaKV,
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
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
