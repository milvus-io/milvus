package streamingcoord

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		// catalog should be reliable to write, ensure the data is consistent in memory and underlying meta storage.
		metaKV: kv.NewReliableWriteMetaKv(metaKV),
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
}

// GetCChannel returns the control channel
func (c *catalog) GetCChannel(ctx context.Context) (*streamingpb.CChannelMeta, error) {
	value, needRepair, found, err := c.loadMetaWithLegacyTrailingSlash(ctx, CChannelMetaKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	info := &streamingpb.CChannelMeta{}
	if err = proto.Unmarshal([]byte(value), info); err != nil {
		return nil, errors.Wrapf(err, "unmarshal cchannel meta failed")
	}
	if needRepair {
		if err = c.saveAndRemoveLegacyMeta(ctx, CChannelMetaKey, value); err != nil {
			return nil, err
		}
	}
	return info, nil
}

// SaveCChannel saves the control channel
func (c *catalog) SaveCChannel(ctx context.Context, info *streamingpb.CChannelMeta) error {
	v, err := proto.Marshal(info)
	if err != nil {
		return errors.Wrapf(err, "marshal cchannel meta failed")
	}
	return c.saveAndRemoveLegacyMeta(ctx, CChannelMetaKey, string(v))
}

// GetVersion returns the streaming version
func (c *catalog) GetVersion(ctx context.Context) (*streamingpb.StreamingVersion, error) {
	value, needRepair, found, err := c.loadMetaWithLegacyTrailingSlash(ctx, VersionKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	info := &streamingpb.StreamingVersion{}
	if err = proto.Unmarshal([]byte(value), info); err != nil {
		return nil, errors.Wrapf(err, "unmarshal streaming version failed")
	}
	if needRepair {
		if err = c.saveAndRemoveLegacyMeta(ctx, VersionKey, value); err != nil {
			return nil, err
		}
	}
	return info, nil
}

// SaveVersion saves the streaming version
func (c *catalog) SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion) error {
	if version == nil {
		return merr.WrapErrServiceInternalMsg("version is nil")
	}
	v, err := proto.Marshal(version)
	if err != nil {
		return errors.Wrapf(err, "marshal streaming version failed")
	}
	return c.saveAndRemoveLegacyMeta(ctx, VersionKey, string(v))
}

func (c *catalog) loadMetaWithLegacyTrailingSlash(ctx context.Context, key string) (string, bool, bool, error) {
	// Callers must serialize Get/Save for key; read repair is not CAS.
	keys, values, err := c.metaKV.LoadWithPrefix(ctx, key)
	if err != nil {
		return "", false, false, err
	}
	var canonicalValue, legacyValue string
	foundCanonical, foundLegacy := false, false
	for i, loadedKey := range keys {
		switch {
		case strings.HasSuffix(loadedKey, key):
			canonicalValue = values[i]
			foundCanonical = true
		case strings.HasSuffix(loadedKey, key+"/"):
			legacyValue = values[i]
			foundLegacy = true
		}
	}
	if foundCanonical {
		return canonicalValue, foundLegacy, true, nil
	}
	return legacyValue, foundLegacy, foundLegacy, nil
}

func (c *catalog) saveAndRemoveLegacyMeta(ctx context.Context, key, value string) error {
	// canonical save recorded before the legacy-key removal, so even a chunked
	// flush can never drop the only copy of the value.
	b := txn.New()
	b.Save(key, value)
	b.Remove(key + "/")
	return txn.Commit(ctx, c.metaKV, b)
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
	b := txn.New()
	for _, info := range infos {
		v, err := proto.Marshal(info)
		if err != nil {
			return errors.Wrapf(err, "marshal pchannel %s failed", info.GetChannel().GetName())
		}
		b.Save(buildPChannelInfoPath(info.GetChannel().GetName()), string(v))
	}
	// within the store's txn limit the batch is one atomic txn; over it, the
	// engine flushes chunks in the input-slice order
	return txn.Commit(ctx, c.metaKV, b)
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
	b := txn.New()
	b.Save(key, string(v))
	return txn.Commit(ctx, c.metaKV, b)
}

// buildPChannelInfoPath builds the path for pchannel info.
func buildPChannelInfoPath(name string) string {
	return PChannelMetaPrefix + name
}

// buildBroadcastTaskPath builds the path for broadcast task.
func buildBroadcastTaskPath(id uint64) string {
	return BroadcastTaskPrefix + strconv.FormatUint(id, 10)
}

// SaveReplicateConfiguration persists the replicate configuration and the new
// replicating-pchannel records as one composite commit. The config record is
// the composite's visibility marker: GetReplicateConfiguration is what makes a
// configuration update observable, so on the engine's chunked fallback it
// lands alone in the final guarded txn, after every task record - a crash
// mid-write leaves the OLD config visible with the new task records inert, and
// the caller's retry (serialized by ChannelManager's lock) reissues the same
// composite. The CDC controller watches the replicating-pchannel prefix
// directly, so task records becoming visible before the config is no wider a
// window than today's unordered MultiSave batches had.
func (c *catalog) SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
	v, err := proto.Marshal(config)
	if err != nil {
		return errors.Wrapf(err, "marshal replicate configuration failed")
	}

	b := txn.New()
	for _, task := range replicatingTasks {
		tv, err := proto.Marshal(task)
		if err != nil {
			return errors.Wrapf(err, "marshal replicate pchannel meta failed")
		}
		b.Save(buildReplicatePChannelPath(task.GetTargetCluster().GetClusterId(), task.GetSourceChannelName()), string(tv))
	}
	b.CommitSave(ReplicateConfigurationKey, string(v))
	return txn.Commit(ctx, c.metaKV, b)
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

func BuildReplicatePChannelMetaKey(meta *streamingpb.ReplicatePChannelMeta) string {
	targetClusterID := meta.GetTargetCluster().GetClusterId()
	sourceChannelName := meta.GetSourceChannelName()
	return buildReplicatePChannelPath(targetClusterID, sourceChannelName)
}

func buildReplicatePChannelPath(targetClusterID, sourceChannelName string) string {
	return fmt.Sprintf("%s%s-%s", ReplicatePChannelMetaPrefix, targetClusterID, sourceChannelName)
}
