package streamingnode

import (
	"context"
	"path"
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

// NewCataLog creates a new streaming-node catalog instance.
// It's used to persist the recovery info for a streaming node and wal.
// The catalog is shown as following:
// streamingnode-meta
// └── wal
//
//	├── pchannel-1
//	│   ├── checkpoint
//	│   ├── vchannels
//	│   │   ├── vchannel-1
//	│   │   └── vchannel-2
//	│   └── segment-assign
//	│       ├── 456398247934
//	│       ├── 456398247936
//	│       └── 456398247939
//	└── pchannel-2
//	    ├── checkpoint
//	    ├── vchannels
//	    │   ├── vchannel-1
//	    │   └── vchannel-2
//	    └── segment-assign
//	        ├── 456398247934
//	        ├── 456398247935
//	        └── 456398247938
func NewCataLog(metaKV kv.MetaKv) metastore.StreamingNodeCataLog {
	return &catalog{
		metaKV: metaKV,
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
}

// ListVChannel lists the vchannel info of the pchannel.
func (c *catalog) ListVChannel(ctx context.Context, pchannelName string) ([]*streamingpb.VChannelMeta, error) {
	prefix := buildVChannelMetaPath(pchannelName)
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	infos := make([]*streamingpb.VChannelMeta, 0, len(values))
	for k, value := range values {
		info := &streamingpb.VChannelMeta{}
		if err = proto.Unmarshal([]byte(value), info); err != nil {
			return nil, errors.Wrapf(err, "unmarshal pchannel %s failed", keys[k])
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// SaveVChannels save vchannel on current pchannel.
func (c *catalog) SaveVChannels(ctx context.Context, pchannelName string, vchannels map[string]*streamingpb.VChannelMeta) error {
	kvs := make(map[string]string, len(vchannels))
	removes := make([]string, 0)
	for _, info := range vchannels {
		key := buildVChannelMetaPathOfVChannel(pchannelName, info.GetVchannel())
		if info.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
			// Flushed segment should be removed from meta
			removes = append(removes, key)
			continue
		}

		data, err := proto.Marshal(info)
		if err != nil {
			return errors.Wrapf(err, "marshal vchannel %d at pchannel %s failed", info.GetVchannel(), pchannelName)
		}
		kvs[key] = string(data)
	}

	if len(removes) > 0 {
		if err := etcd.RemoveByBatchWithLimit(removes, util.MaxEtcdTxnNum, func(partialRemoves []string) error {
			return c.metaKV.MultiRemove(ctx, partialRemoves)
		}); err != nil {
			return err
		}
	}
	if len(kvs) > 0 {
		return etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, func(partialKvs map[string]string) error {
			return c.metaKV.MultiSave(ctx, partialKvs)
		})
	}
	return nil
}

// ListSegmentAssignment lists the segment assignment info of the pchannel.
func (c *catalog) ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error) {
	prefix := buildSegmentAssignmentMetaPath(pChannelName)
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	infos := make([]*streamingpb.SegmentAssignmentMeta, 0, len(values))
	for k, value := range values {
		info := &streamingpb.SegmentAssignmentMeta{}
		if err = proto.Unmarshal([]byte(value), info); err != nil {
			return nil, errors.Wrapf(err, "unmarshal pchannel %s failed", keys[k])
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// SaveSegmentAssignments saves the segment assignment info to meta storage.
func (c *catalog) SaveSegmentAssignments(ctx context.Context, pChannelName string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error {
	kvs := make(map[string]string, len(infos))
	removes := make([]string, 0)
	for _, info := range infos {
		key := buildSegmentAssignmentMetaPathOfSegment(pChannelName, info.GetSegmentId())
		if info.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
			// Flushed segment should be removed from meta
			removes = append(removes, key)
			continue
		}

		data, err := proto.Marshal(info)
		if err != nil {
			return errors.Wrapf(err, "marshal segment %d at pchannel %s failed", info.GetSegmentId(), pChannelName)
		}
		kvs[key] = string(data)
	}

	if len(removes) > 0 {
		if err := etcd.RemoveByBatchWithLimit(removes, util.MaxEtcdTxnNum, func(partialRemoves []string) error {
			return c.metaKV.MultiRemove(ctx, partialRemoves)
		}); err != nil {
			return err
		}
	}

	if len(kvs) > 0 {
		return etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, func(partialKvs map[string]string) error {
			return c.metaKV.MultiSave(ctx, partialKvs)
		})
	}
	return nil
}

// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
func (c *catalog) GetConsumeCheckpoint(ctx context.Context, pchannelName string) (*streamingpb.WALCheckpoint, error) {
	key := buildConsumeCheckpointPath(pchannelName)
	value, err := c.metaKV.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	val := &streamingpb.WALCheckpoint{}
	if err = proto.Unmarshal([]byte(value), val); err != nil {
		return nil, err
	}
	return val, nil
}

// SaveConsumeCheckpoint saves the consuming checkpoint of the wal.
func (c *catalog) SaveConsumeCheckpoint(ctx context.Context, pchannelName string, checkpoint *streamingpb.WALCheckpoint) error {
	key := buildConsumeCheckpointPath(pchannelName)
	value, err := proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return c.metaKV.Save(ctx, key, string(value))
}

// buildVChannelMetaPath builds the path for vchannel meta
func buildVChannelMetaPath(pChannelName string) string {
	return path.Join(buildWALDirectory(pChannelName), DirectoryVChannel) + "/"
}

// buildVChannelMetaPathOfVChannel builds the path for vchannel meta
func buildVChannelMetaPathOfVChannel(pChannelName string, vchannelName string) string {
	return path.Join(buildVChannelMetaPath(pChannelName), vchannelName)
}

// buildSegmentAssignmentMetaPath builds the path for segment assignment
func buildSegmentAssignmentMetaPath(pChannelName string) string {
	return path.Join(buildWALDirectory(pChannelName), DirectorySegmentAssign) + "/"
}

// buildSegmentAssignmentMetaPathOfSegment builds the path for segment assignment
func buildSegmentAssignmentMetaPathOfSegment(pChannelName string, segmentID int64) string {
	return path.Join(buildWALDirectory(pChannelName), DirectorySegmentAssign, strconv.FormatInt(segmentID, 10))
}

// buildConsumeCheckpointPath builds the path for consume checkpoint
func buildConsumeCheckpointPath(pchannelName string) string {
	return path.Join(buildWALDirectory(pchannelName), KeyConsumeCheckpoint)
}

// buildWALDirectory builds the path for wal directory
func buildWALDirectory(pchannelName string) string {
	return path.Join(MetaPrefix, DirectoryWAL, pchannelName) + "/"
}
