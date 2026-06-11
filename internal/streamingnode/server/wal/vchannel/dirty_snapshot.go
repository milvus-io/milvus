package vchannel

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
)

type dirtySnapshot struct {
	key          moduleapi.SnapshotKey
	op           moduleapi.SnapshotOp
	payload      proto.Message
	metaTimeTick uint64
	mark         func()
}

func newDirtySnapshot(
	key moduleapi.SnapshotKey,
	op moduleapi.SnapshotOp,
	payload proto.Message,
	metaTimeTick uint64,
	mark func(),
) *dirtySnapshot {
	return &dirtySnapshot{
		key:          key,
		op:           op,
		payload:      payload,
		metaTimeTick: metaTimeTick,
		mark:         mark,
	}
}

func (s *dirtySnapshot) ModuleName() moduleapi.ModuleName {
	return moduleapi.ModuleNameVChannel
}

func (s *dirtySnapshot) Key() moduleapi.SnapshotKey {
	return s.key
}

func (s *dirtySnapshot) Op() moduleapi.SnapshotOp {
	return s.op
}

func (s *dirtySnapshot) Payload() proto.Message {
	return s.payload
}

func (s *dirtySnapshot) MetaTimeTick() uint64 {
	return s.metaTimeTick
}

func (s *dirtySnapshot) DataTimeTick() uint64 {
	return 0
}

func (s *dirtySnapshot) MarkPersisted() {
	if s.mark != nil {
		s.mark()
	}
}

var _ moduleapi.DirtySnapshot = (*dirtySnapshot)(nil)
