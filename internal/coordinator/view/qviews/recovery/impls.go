package recovery

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// TODO: should be renamed in future after coordinator refactor before release.
const recoveryStoragePerfix = "coordinator/qview"

var _ RecoveryStorage = (*recoveryStorageImpl)(nil)

// NewRecoveryStorage creates a new RecoveryStorage instance.
func NewRecoveryStorage(metaKV kv.MetaKv) RecoveryStorage {
	return &recoveryStorageImpl{
		metaKV: metaKV,
	}
}

// recoveryStorageImpl is the implementation of RecoveryStorage.
type recoveryStorageImpl struct {
	metaKV kv.MetaKv
}

// List returns all the query view of shards.
func (rs *recoveryStorageImpl) List(ctx context.Context) ([]*viewpb.QueryViewOfShard, error) {
	_, vals, err := rs.metaKV.LoadWithPrefix(ctx, "")
	if err != nil {
		return nil, err
	}
	views := make([]*viewpb.QueryViewOfShard, 0, len(vals))
	for _, val := range vals {
		v := &viewpb.QueryViewOfShard{}
		mustUnmarshal(val, v)
		views = append(views, v)
	}
	return views, nil
}

// Save saves the recovery infos into underlying persist storage.
// The operation should be executed atomically.
func (rs *recoveryStorageImpl) Save(ctx context.Context, saved ...*viewpb.QueryViewOfShard) error {
	if len(saved) == 0 {
		return nil
	}
	saves := make(map[string]string, len(saved))
	deletes := make([]string, 0, len(saved))

	ev := events.RecoveryEventPersisted{
		Persisted: make([]events.PersistedItem, 0, len(saved)),
	}
	for _, s := range saved {
		if s.Meta.State == viewpb.QueryViewState_QueryViewStateDropped {
			deletes = append(deletes, rs.getPath(s.Meta))
			continue
		}
		saves[rs.getPath(s.Meta)] = mustMarshal(s)
		ev.Persisted = append(ev.Persisted, events.PersistedItem{
			ShardID: qviews.NewShardIDFromQVMeta(s.Meta),
			Version: qviews.FromProtoQueryViewVersion(s.Meta.Version),
			State:   qviews.QueryViewState(s.Meta.State),
		})
	}
	for {
		// Save the recovery info until success or context canceled.
		err := rs.metaKV.MultiSaveAndRemove(ctx, saves, deletes)
		if err == nil {
			// Notify the event after the recovery info is saved.
			events.Notify(ev)
			return nil
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
	}
}

// getPath returns the path of the query view of shard.
func (r *recoveryStorageImpl) getPath(metas *viewpb.QueryViewMeta) (key string) {
	return fmt.Sprintf("%s/%d/%d/%s/%d,%d", recoveryStoragePerfix, metas.CollectionId, metas.ReplicaId, metas.Vchannel, metas.Version.DataVersion, metas.Version.QueryVersion)
}

// mustMarshal marshals the proto message to string.
func mustMarshal(m proto.Message) string {
	bytes, err := proto.Marshal(m)
	if err != nil {
		panic("marshal meta fields failed")
	}
	return string(bytes)
}

// mustUnmarshal unmarshals the string to proto message.
func mustUnmarshal(data string, m proto.Message) {
	if err := proto.Unmarshal([]byte(data), m); err != nil {
		panic("unmarshal meta fields failed")
	}
}
