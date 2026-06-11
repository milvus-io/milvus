package transformlog

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
)

type moduleDirtySnapshot struct {
	key          moduleapi.SnapshotKey
	op           moduleapi.SnapshotOp
	payload      proto.Message
	dataTimeTick uint64
	mark         func()
}

func newModuleDirtySnapshot(
	key moduleapi.SnapshotKey,
	op moduleapi.SnapshotOp,
	payload proto.Message,
	dataTimeTick uint64,
	mark func(),
) *moduleDirtySnapshot {
	return &moduleDirtySnapshot{
		key:          key,
		op:           op,
		payload:      payload,
		dataTimeTick: dataTimeTick,
		mark:         mark,
	}
}

func (s *moduleDirtySnapshot) ModuleName() moduleapi.ModuleName {
	return moduleapi.ModuleNameTransformLog
}

func (s *moduleDirtySnapshot) Key() moduleapi.SnapshotKey {
	return s.key
}

func (s *moduleDirtySnapshot) Op() moduleapi.SnapshotOp {
	return s.op
}

func (s *moduleDirtySnapshot) Payload() proto.Message {
	return s.payload
}

func (s *moduleDirtySnapshot) MetaTimeTick() uint64 {
	return 0
}

func (s *moduleDirtySnapshot) DataTimeTick() uint64 {
	return s.dataTimeTick
}

func (s *moduleDirtySnapshot) MarkPersisted() {
	if s.mark != nil {
		s.mark()
	}
}

var _ moduleapi.DirtySnapshot = (*moduleDirtySnapshot)(nil)
