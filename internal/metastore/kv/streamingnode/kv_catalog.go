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
//	│   ├── window-store
//	│   │   ├── pchannel-meta
//	│   │   └── vchannels
//	│   │       └── idempotency
//	│   │           ├── vchannel-1
//	│   │           └── vchannel-2
//	│   └── segment-assign
//	│       ├── 456398247934
//	│       ├── 456398247936
//	│       └── 456398247939
//	└── pchannel-2
//	    ├── checkpoint
//	    ├── vchannels
//	    │   ├── vchannel-1
//	    │   └── vchannel-2
//	    ├── window-store
//	    │   ├── pchannel-meta
//	    │   └── vchannels
//	    │       └── idempotency
//	    │           ├── vchannel-1
//	    │           └── vchannel-2
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
			panic(fmt.Sprintf("vchannel %s has no schemas in recovery info", vchannelName))
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

// getRemovalAndSaveForVChannel gets the removal and save for vchannel.
func (c *catalog) getRemovalAndSaveForVChannel(pchannelName string, info *streamingpb.VChannelMeta) ([]string, map[string]string, error) {
	removes := make([]string, 0, len(info.CollectionInfo.Schemas)+1)
	kvs := make(map[string]string, len(info.CollectionInfo.Schemas)+1)

	key := buildVChannelKey(pchannelName, info.GetVchannel())
	if info.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		// Dropped vchannel should be removed from meta
		for _, schema := range info.GetCollectionInfo().GetSchemas() {
			// Also remove the schema of the vchannel.
			removes = append(removes, buildVChannelSchemaKey(pchannelName, info.GetVchannel(), schema.GetCheckpointTimeTick()))
		}
		removes = append(removes, key)
		return removes, kvs, nil
	}

	// Save the schema of the vchannel.
	for _, schema := range info.GetCollectionInfo().GetSchemas() {
		switch schema.State {
		case streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED:
			// Dropped schema should be removed from meta
			removes = append(removes, buildVChannelSchemaKey(pchannelName, info.GetVchannel(), schema.GetCheckpointTimeTick()))
		default:
			data, err := proto.Marshal(schema)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "marshal schema %d at pchannel %s failed", schema.GetCheckpointTimeTick(), pchannelName)
			}
			kvs[buildVChannelSchemaKey(pchannelName, info.GetVchannel(), schema.GetCheckpointTimeTick())] = string(data)
		}
	}
	// Schema is saved in the other key, so we don't need to save it in the vchannel meta.
	// swap it first to marshal the vchannel meta without schema.
	oldSchema := info.CollectionInfo.Schemas
	info.CollectionInfo.Schemas = nil
	data, err := proto.Marshal(info)
	info.CollectionInfo.Schemas = oldSchema
	if err != nil {
		return nil, nil, errors.Wrapf(err, "marshal vchannel %d at pchannel %s failed", info.GetVchannel(), pchannelName)
	}
	kvs[key] = string(data)
	return removes, kvs, nil
}

// ListVChannelWindowMetas lists the window metadata of the pchannel for the given view type.
func (c *catalog) ListVChannelWindowMetas(ctx context.Context, pchannelName string, viewType string) ([]*streamingpb.VChannelWindowMeta, error) {
	if viewType == "" {
		return nil, errors.New("vchannel window meta view type is empty")
	}
	prefix := buildVChannelWindowMetaPrefix(pchannelName, viewType)
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	metas := make([]*streamingpb.VChannelWindowMeta, 0, len(values))
	for i, value := range values {
		meta := &streamingpb.VChannelWindowMeta{}
		if err := proto.Unmarshal([]byte(value), meta); err != nil {
			return nil, errors.Wrapf(err, "unmarshal vchannel window meta %s failed", keys[i])
		}
		// LoadWithPrefix returns full keys including the metaKV rootPath, so strip the
		// prefix rootPath-tolerantly like removePrefix does; a plain strings.TrimPrefix
		// would be a no-op and leave the whole key as the vchannel name.
		vchannelName := typeutil.After(keys[i], prefix)
		meta, _, err = normalizeVChannelWindowMeta(pchannelName, viewType, vchannelName, meta)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid vchannel window meta %s", keys[i])
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

// SaveVChannelWindowMetas saves the window metadata of the pchannel for the given view type.
func (c *catalog) SaveVChannelWindowMetas(ctx context.Context, pchannelName string, viewType string, windows map[string]*streamingpb.VChannelWindowMeta) error {
	if viewType == "" {
		return errors.New("vchannel window meta view type is empty")
	}
	kvs := make(map[string]string, len(windows))
	for vchannel, meta := range windows {
		if meta == nil {
			continue
		}
		stored, vchannelName, err := normalizeVChannelWindowMeta(pchannelName, viewType, vchannel, meta)
		if err != nil {
			return errors.Wrapf(err, "invalid vchannel window meta %s at pchannel %s view %s", vchannel, pchannelName, viewType)
		}
		data, err := proto.Marshal(stored)
		if err != nil {
			return errors.Wrapf(err, "marshal vchannel window meta %s at pchannel %s view %s failed", vchannelName, pchannelName, viewType)
		}
		kvs[buildVChannelWindowMetaKey(pchannelName, viewType, vchannelName)] = string(data)
	}

	if len(kvs) == 0 {
		return nil
	}
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	return etcd.SaveByBatchWithLimit(kvs, maxTxnNum, func(partialKvs map[string]string) error {
		return c.metaKV.MultiSave(ctx, partialKvs)
	})
}

// RemoveVChannelWindowMetas removes the window metadata of the pchannel for the given view type.
func (c *catalog) RemoveVChannelWindowMetas(ctx context.Context, pchannelName string, viewType string, vchannels []string) error {
	if viewType == "" {
		return errors.New("vchannel window meta view type is empty")
	}
	if len(vchannels) == 0 {
		return nil
	}
	removes := make([]string, 0, len(vchannels))
	for _, vchannel := range vchannels {
		removes = append(removes, buildVChannelWindowMetaKey(pchannelName, viewType, vchannel))
	}
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	return etcd.RemoveByBatchWithLimit(removes, maxTxnNum, func(partialRemoves []string) error {
		return c.metaKV.MultiRemove(ctx, partialRemoves)
	})
}

// GetPChannelWindowMeta gets the pchannel-level physical window metadata.
func (c *catalog) GetPChannelWindowMeta(ctx context.Context, pchannelName string) (*streamingpb.PChannelWindowMeta, error) {
	key := buildPChannelWindowMetaKey(pchannelName)
	value, err := c.metaKV.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	meta := &streamingpb.PChannelWindowMeta{}
	if err := proto.Unmarshal([]byte(value), meta); err != nil {
		return nil, errors.Wrapf(err, "unmarshal pchannel window meta %s failed", key)
	}
	meta, err = normalizePChannelWindowMeta(pchannelName, meta)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pchannel window meta %s", key)
	}
	return meta, nil
}

// SavePChannelWindowMeta saves the pchannel-level physical window metadata.
func (c *catalog) SavePChannelWindowMeta(ctx context.Context, pchannelName string, meta *streamingpb.PChannelWindowMeta) error {
	if meta == nil {
		return nil
	}
	stored, err := normalizePChannelWindowMeta(pchannelName, meta)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(stored)
	if err != nil {
		return errors.Wrapf(err, "marshal pchannel window meta at pchannel %s failed", pchannelName)
	}
	return c.metaKV.Save(ctx, buildPChannelWindowMetaKey(pchannelName), string(data))
}

func normalizeVChannelWindowMeta(pchannelName string, viewType string, vchannelName string, meta *streamingpb.VChannelWindowMeta) (*streamingpb.VChannelWindowMeta, string, error) {
	if meta == nil {
		return nil, "", errors.New("nil vchannel window meta")
	}
	if viewType == "" {
		return nil, "", errors.New("vchannel window meta view type is empty")
	}
	stored := proto.Clone(meta).(*streamingpb.VChannelWindowMeta)
	if stored.GetPchannel() == "" {
		stored.Pchannel = pchannelName
	} else if stored.GetPchannel() != pchannelName {
		return nil, "", errors.Errorf("pchannel mismatch: path %s, meta %s", pchannelName, stored.GetPchannel())
	}
	if stored.GetViewType() == "" {
		stored.ViewType = viewType
	} else if stored.GetViewType() != viewType {
		return nil, "", errors.Errorf("view type mismatch: path %s, meta %s", viewType, stored.GetViewType())
	}
	if stored.GetVchannel() == "" {
		if vchannelName == "" {
			return nil, "", errors.New("vchannel is empty")
		}
		stored.Vchannel = vchannelName
	} else {
		if vchannelName != "" && stored.GetVchannel() != vchannelName {
			return nil, "", errors.Errorf("vchannel mismatch: path %s, meta %s", vchannelName, stored.GetVchannel())
		}
		vchannelName = stored.GetVchannel()
	}
	return stored, vchannelName, nil
}

func normalizePChannelWindowMeta(pchannelName string, meta *streamingpb.PChannelWindowMeta) (*streamingpb.PChannelWindowMeta, error) {
	if meta == nil {
		return nil, errors.New("nil pchannel window meta")
	}
	stored := proto.Clone(meta).(*streamingpb.PChannelWindowMeta)
	if stored.GetPchannel() == "" {
		stored.Pchannel = pchannelName
	} else if stored.GetPchannel() != pchannelName {
		return nil, errors.Errorf("pchannel mismatch: path %s, meta %s", pchannelName, stored.GetPchannel())
	}
	return stored, nil
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

// SaveConsumeCheckpoint saves the consuming checkpoint of the wal.
func (c *catalog) SaveConsumeCheckpoint(ctx context.Context, pchannelName string, checkpoint *streamingpb.WALCheckpoint) error {
	key := buildConsumeCheckpointKey(pchannelName)
	value, err := proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return c.metaKV.Save(ctx, key, string(value))
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

// buildWindowStorePrefix returns the prefix for all physical window store metadata under a pchannel.
func buildWindowStorePrefix(pchannelName string) string {
	return buildWALPrefix(pchannelName) + DirectoryWindowStore + "/"
}

// buildVChannelWindowMetaPrefix returns the prefix for all vchannel window metadata of a view type under a pchannel.
func buildVChannelWindowMetaPrefix(pchannelName string, viewType string) string {
	return buildWindowStorePrefix(pchannelName) + DirectoryWindowVChannel + "/" + viewType + "/"
}

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

// buildVChannelWindowMetaKey returns the key for a specific vchannel window metadata under a view type.
func buildVChannelWindowMetaKey(pchannelName string, viewType string, vchannelName string) string {
	return buildVChannelWindowMetaPrefix(pchannelName, viewType) + vchannelName
}

// buildPChannelWindowMetaKey returns the key for pchannel-level physical window metadata.
func buildPChannelWindowMetaKey(pchannelName string) string {
	return buildWindowStorePrefix(pchannelName) + KeyPChannelWindowMeta
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
