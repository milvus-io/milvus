package streamingnode

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
//	│   │   │   ├── schema/version-1
//	│   │   │   └── schema/version-2
//	│   │   ├── vchannel-2
//	│   │   │   └── schema/version-1
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
	prefix := buildVChannelPrefix(pchannelName)
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return c.newVChannelMetaFromKV(prefix, keys, values)
}

// newVChannelMetaFromKV groups the vchannel meta by the vchannel name.
func (c *catalog) newVChannelMetaFromKV(prefix string, keys []string, values []string) ([]*streamingpb.VChannelMeta, error) {
	keys = removePrefix(prefix, keys)
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(keys))
	schemas := make(map[string][]*streamingpb.CollectionSchemaOfVChannel, len(keys))
	for idx, key := range keys {
		ks := strings.Split(key, "/")
		switch len(ks) {
		case 1:
			// the vchannel vchannel path.
			vchannel := &streamingpb.VChannelMeta{}
			if err := proto.Unmarshal([]byte(values[idx]), vchannel); err != nil {
				return nil, errors.Wrapf(err, "unmarshal vchannel meta %s failed", key)
			}
			if vchannel.GetVchannel() != ks[0] {
				return nil, merr.WrapErrDataIntegrityMsg("mismatched vchannel recovery meta, key %s, meta %s", ks[0], vchannel.GetVchannel())
			}
			vchannels[ks[0]] = vchannel
		case 3: // {{vchannel}}/schema/{{version}}
			// the schema path.
			channelName := ks[0]
			if ks[1] != DirectorySchema {
				continue
			}
			schema := &streamingpb.CollectionSchemaOfVChannel{}
			if err := proto.Unmarshal([]byte(values[idx]), schema); err != nil {
				return nil, errors.Wrapf(err, "unmarshal schema %s failed", key)
			}
			if _, ok := schemas[channelName]; !ok {
				schemas[channelName] = make([]*streamingpb.CollectionSchemaOfVChannel, 0, 2)
			}
			schemas[channelName] = append(schemas[channelName], schema)
		}
	}
	vchannelsWithSchemas := make([]*streamingpb.VChannelMeta, 0, len(vchannels))
	for vchannelName, vchannel := range vchannels {
		schemas, ok := schemas[vchannelName]
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("vchannel %s missing schemas in recovery info", vchannelName)
		}
		sort.Slice(schemas, func(i, j int) bool {
			// order by checkpoint time tick.
			return schemas[i].CheckpointTimeTick < schemas[j].CheckpointTimeTick
		})
		vchannel.CollectionInfo.Schemas = schemas
		vchannelsWithSchemas = append(vchannelsWithSchemas, vchannel)
	}
	return vchannelsWithSchemas, nil
}

// ListTransformLogMeta lists transform log metas of the pchannel.
func (c *catalog) ListTransformLogMeta(ctx context.Context, pchannelName string) (map[string]*streamingpb.VChannelTransformLogMeta, error) {
	prefix := buildTransformLogPrefix(pchannelName)
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	metas := make(map[string]*streamingpb.VChannelTransformLogMeta, len(values))
	for idx, value := range values {
		vchannel, err := parseCompactVChannelKey(keys[idx], prefix, pchannelName)
		if err != nil {
			return nil, err
		}
		meta := &streamingpb.VChannelTransformLogMeta{}
		if err := proto.Unmarshal([]byte(value), meta); err != nil {
			return nil, errors.Wrapf(err, "unmarshal transform log meta %s failed", keys[idx])
		}
		metas[vchannel] = meta
	}
	return metas, nil
}

// getRemovalAndSaveForVChannel gets the removal and save for vchannel.
func (c *catalog) getRemovalAndSaveForVChannel(pchannelName string, info *streamingpb.VChannelMeta) ([]string, map[string]string, error) {
	removes := make([]string, 0, len(info.CollectionInfo.Schemas)+1)
	kvs := make(map[string]string, len(info.CollectionInfo.Schemas)+1)

	key := buildVChannelKey(pchannelName, info.GetVchannel())
	// Save the schema of the vchannel.
	for _, schema := range info.GetCollectionInfo().GetSchemas() {
		switch schema.State {
		case streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL:
			data, err := proto.Marshal(schema)
			if err != nil {
				return nil, nil, merr.WrapErrSerializationFailed(err, "marshal schema %d at pchannel %s", schema.GetCheckpointTimeTick(), pchannelName)
			}
			kvs[buildVChannelSchemaKey(pchannelName, info.GetVchannel(), schema.GetCheckpointTimeTick())] = string(data)
		default:
			return nil, nil, merr.WrapErrDataIntegrityMsg("unknown vchannel schema state in recovery meta: vchannel %s schema %d", info.GetVchannel(), schema.GetCheckpointTimeTick())
		}
	}
	data, err := marshalVChannelBaseMeta(pchannelName, info)
	if err != nil {
		return nil, nil, err
	}
	kvs[key] = data
	return removes, kvs, nil
}

func marshalVChannelBaseMeta(pchannelName string, info *streamingpb.VChannelMeta) (string, error) {
	// Schema is saved in separate keys. The caller passes a stable snapshot, so
	// temporarily excluding it avoids an additional full-meta clone.
	oldSchemas := info.CollectionInfo.Schemas
	info.CollectionInfo.Schemas = nil
	data, err := proto.Marshal(info)
	info.CollectionInfo.Schemas = oldSchemas
	if err != nil {
		return "", merr.WrapErrSerializationFailed(err, "marshal vchannel %s at pchannel %s", info.GetVchannel(), pchannelName)
	}
	return string(data), nil
}

// ListSegmentAssignment lists the segment assignment info of the pchannel.
func (c *catalog) ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error) {
	prefix := buildSegmentAssignmentPrefix(pChannelName)
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
		segmentID, err := strconv.ParseInt(typeutil.After(keys[k], prefix), 10, 64)
		if err != nil || segmentID != info.GetSegmentId() {
			return nil, merr.WrapErrDataIntegrityMsg("mismatched segment assignment recovery meta, key %s, meta %d", keys[k], info.GetSegmentId())
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
func (c *catalog) GetConsumeCheckpoint(ctx context.Context, pchannelName string) (*streamingpb.WALCheckpoint, error) {
	key := buildConsumeCheckpointKey(pchannelName)
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

// GetSalvageCheckpoint gets all salvage checkpoints for a channel (one per source cluster).
func (c *catalog) GetSalvageCheckpoint(ctx context.Context, pchannelName string) ([]*commonpb.ReplicateCheckpoint, error) {
	prefix := buildSalvageCheckpointPrefix(pchannelName)
	_, values, err := c.metaKV.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	checkpoints := make([]*commonpb.ReplicateCheckpoint, 0, len(values))
	for _, value := range values {
		val := &commonpb.ReplicateCheckpoint{}
		if err = proto.Unmarshal([]byte(value), val); err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, val)
	}
	return checkpoints, nil
}

// Prefix functions: return paths ending with "/" for LoadWithPrefix queries.

// buildWALPrefix returns the prefix for all WAL metadata under a pchannel.
func buildWALPrefix(pchannelName string) string {
	return MetaPrefix + "/" + DirectoryWAL + "/" + pchannelName + "/"
}

// buildVChannelPrefix returns the prefix for all vchannel metadata under a pchannel.
func buildVChannelPrefix(pChannelName string) string {
	return buildWALPrefix(pChannelName) + DirectoryVChannel + "/"
}

// buildSegmentAssignmentPrefix returns the prefix for all segment assignment metadata under a pchannel.
func buildSegmentAssignmentPrefix(pChannelName string) string {
	return buildWALPrefix(pChannelName) + DirectorySegmentAssign + "/"
}

// buildTransformLogPrefix returns the prefix for transform log metadata under a pchannel.
func buildTransformLogPrefix(pChannelName string) string {
	return buildWALPrefix(pChannelName) + DirectoryTransformLog + "/"
}

// Key functions: return exact keys for individual records.

// Key functions: return exact keys for individual records.

// buildVChannelKey returns the key for a specific vchannel's metadata.
func buildVChannelKey(pChannelName string, vchannelName string) string {
	return buildVChannelPrefix(pChannelName) + vchannelName
}

// buildVChannelSchemaKey returns the key for a specific vchannel schema version.
func buildVChannelSchemaKey(pChannelName string, vchannelName string, version uint64) string {
	return buildVChannelKey(pChannelName, vchannelName) + "/" + DirectorySchema + "/" + strconv.FormatUint(version, 10)
}

// buildSegmentAssignmentKey returns the key for a specific segment assignment.
func buildSegmentAssignmentKey(pChannelName string, segmentID int64) string {
	return buildSegmentAssignmentPrefix(pChannelName) + strconv.FormatInt(segmentID, 10)
}

// buildTransformLogKey returns the key for a specific transform log's metadata.
func buildTransformLogKey(pChannelName string, vchannelName string) (string, error) {
	return buildCompactVChannelKey(buildTransformLogPrefix(pChannelName), pChannelName, vchannelName)
}

func buildCompactVChannelKey(prefix string, pchannelName string, vchannelName string) (string, error) {
	pchannel, collectionID, vchannelIndex, err := funcutil.ParseVChannel(vchannelName)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "parse recovery vchannel %s", vchannelName)
	}
	if pchannel != pchannelName {
		return "", merr.WrapErrServiceInternalMsg(
			"vchannel %s pchannel %s mismatches catalog pchannel %s",
			vchannelName,
			pchannel,
			pchannelName,
		)
	}
	return fmt.Sprintf("%s%d/%d", prefix, collectionID, vchannelIndex), nil
}

func parseCompactVChannelKey(key string, prefix string, pchannelName string) (string, error) {
	components := strings.Split(typeutil.After(key, prefix), "/")
	if len(components) != 2 || components[0] == "" || components[1] == "" {
		return "", merr.WrapErrDataIntegrityMsg("malformed compact vchannel metadata key %s", key)
	}
	collectionID, err := strconv.ParseInt(components[0], 10, 64)
	if err != nil || collectionID < 0 || strconv.FormatInt(collectionID, 10) != components[0] {
		return "", merr.WrapErrDataIntegrityMsg("malformed compact vchannel metadata key %s", key)
	}
	vchannelIndex, err := strconv.ParseInt(components[1], 10, strconv.IntSize)
	if err != nil || vchannelIndex < 0 || strconv.FormatInt(vchannelIndex, 10) != components[1] {
		return "", merr.WrapErrDataIntegrityMsg("malformed compact vchannel metadata key %s", key)
	}
	return funcutil.GetVirtualChannel(pchannelName, collectionID, int(vchannelIndex)), nil
}

// buildConsumeCheckpointKey returns the key for the consume checkpoint of a pchannel.
func buildConsumeCheckpointKey(pchannelName string) string {
	return buildWALPrefix(pchannelName) + KeyConsumeCheckpoint
}

// removePrefix removes the prefix from the keys.
func removePrefix(prefix string, keys []string) []string {
	for idx, key := range keys {
		keys[idx] = typeutil.After(key, prefix)
	}
	return keys
}

// buildSalvageCheckpointPrefix builds the prefix for all salvage checkpoints under a pchannel.
func buildSalvageCheckpointPrefix(pchannelName string) string {
	return buildWALPrefix(pchannelName) + KeySalvageCheckpoint + "/"
}

// buildSalvageCheckpointPath builds the path for salvage checkpoint for a specific source cluster.
func buildSalvageCheckpointPath(pchannelName, sourceClusterID string) string {
	return buildSalvageCheckpointPrefix(pchannelName) + sourceClusterID
}
