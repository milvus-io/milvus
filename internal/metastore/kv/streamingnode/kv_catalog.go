package streamingnode

import (
	"context"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

// NewCataLog creates a new catalog instance
func NewCataLog(metaKV kv.MetaKv) metastore.StreamingNodeCataLog {
	return &catalog{
		metaKV: metaKV,
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
}

func (c *catalog) ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error) {
	prefix := buildSegmentAssignmentMetaPath(pChannelName)
	keys, values, err := c.metaKV.LoadWithPrefix(prefix)
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
func (c *catalog) SaveSegmentAssignments(ctx context.Context, pChannelName string, infos []*streamingpb.SegmentAssignmentMeta) error {
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
			return c.metaKV.MultiRemove(partialRemoves)
		}); err != nil {
			return err
		}
	}

	if len(kvs) > 0 {
		return etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, func(partialKvs map[string]string) error {
			return c.metaKV.MultiSave(partialKvs)
		})
	}
	return nil
}

// buildSegmentAssignmentMetaPath builds the path for segment assignment
// streamingnode-meta/segment-assign/${pChannelName}
func buildSegmentAssignmentMetaPath(pChannelName string) string {
	// !!! bad implementation here, but we can't make compatibility for underlying meta kv.
	// underlying meta kv will remove the last '/' of the path, cause the pchannel lost.
	// So we add a special sub path to avoid this.
	return path.Join(SegmentAssignMeta, pChannelName, SegmentAssignSubFolder) + "/"
}

// buildSegmentAssignmentMetaPathOfSegment builds the path for segment assignment
func buildSegmentAssignmentMetaPathOfSegment(pChannelName string, segmentID int64) string {
	return path.Join(SegmentAssignMeta, pChannelName, SegmentAssignSubFolder, strconv.FormatInt(segmentID, 10))
}
