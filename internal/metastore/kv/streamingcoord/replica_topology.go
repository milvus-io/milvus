package streamingcoord

import (
	"context"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReplicaTopologyCatalog holds the desired state at the PChannel key. Session
// routes and acknowledgements never enter this catalog.
type ReplicaTopologyCatalog struct{ kv kv.MetaKv }

func NewReplicaTopologyCatalog(meta kv.MetaKv) *ReplicaTopologyCatalog {
	return &ReplicaTopologyCatalog{kv: meta}
}

func (c *ReplicaTopologyCatalog) List(ctx context.Context) (map[string]*streamingpb.WALReplicaTopology, error) {
	keys, values, err := c.kv.LoadWithPrefix(ctx, PChannelMetaPrefix)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*streamingpb.WALReplicaTopology, len(values))
	for i, value := range values {
		topology := &streamingpb.WALReplicaTopology{}
		if err := proto.Unmarshal([]byte(value), topology); err != nil {
			return nil, merr.Wrapf(err, "decode WAL replica topology at %s", keys[i])
		}
		if topology.Revision <= 0 || topology.Pchannel == "" || topology.Pchannel != path.Base(keys[i]) {
			return nil, merr.WrapErrDataIntegrityMsg("invalid WAL replica topology at %s", keys[i])
		}
		result[topology.Pchannel] = topology
	}
	return result, nil
}

// Save compares the desired state read by the planner. In particular a stale
// coordinator cannot restore an old membership after a new leader has published.
func (c *ReplicaTopologyCatalog) Save(ctx context.Context, previous, next *streamingpb.WALReplicaTopology) error {
	key := BuildPChannelInfoPath(next.Pchannel)
	guard := predicates.KeyNotExist(key)
	if previous.GetRevision() > 0 {
		value, err := proto.Marshal(previous)
		if err != nil {
			return err
		}
		guard = predicates.ValueEqual(key, string(value))
	}
	value, err := proto.Marshal(next)
	if err != nil {
		return err
	}
	return c.kv.MultiSaveAndRemove(ctx, map[string]string{key: string(value)}, nil, guard)
}

// Get loads the current writer authorization immediately before executing a
// handoff operation. A queued RPC cannot authorize a superseded successor.
func (c *ReplicaTopologyCatalog) Get(ctx context.Context, name string) (*streamingpb.WALReplicaTopology, error) {
	value, err := c.kv.Load(ctx, BuildPChannelInfoPath(name))
	if err != nil {
		return nil, err
	}
	t := &streamingpb.WALReplicaTopology{}
	if err := proto.Unmarshal([]byte(value), t); err != nil {
		return nil, err
	}
	if t.Pchannel != name || t.Revision <= 0 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid WAL replica topology for %s", name)
	}
	return t, nil
}
