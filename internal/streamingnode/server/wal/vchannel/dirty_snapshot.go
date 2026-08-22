package vchannel

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
)

type dirtySnapshot struct {
	moduleName moduleapi.ModuleName
	key        moduleapi.SnapshotKey
	op         moduleapi.SnapshotOp
	payload    proto.Message
	mark       func()
}

func newDirtySnapshot(
	moduleName moduleapi.ModuleName,
	key moduleapi.SnapshotKey,
	op moduleapi.SnapshotOp,
	payload proto.Message,
	mark func(),
) *dirtySnapshot {
	return &dirtySnapshot{
		moduleName: moduleName,
		key:        key,
		op:         op,
		payload:    payload,
		mark:       mark,
	}
}

func (s *dirtySnapshot) ModuleName() moduleapi.ModuleName {
	return s.moduleName
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

func (s *dirtySnapshot) MarkPersisted() {
	if s.mark != nil {
		s.mark()
	}
}

var _ moduleapi.DirtySnapshot = (*dirtySnapshot)(nil)
