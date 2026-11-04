package registry

import (
	"context"
	"sync"
)

// AppendFirstReplicaRecordedChecker reports whether this cluster holds a durable
// record that the append-first replica a gated broadcast waits for has landed in
// its own WAL on vchannel.
//
// It answers the one question the broadcaster cannot answer from its own state
// once a broadcast task has been collected: a secondary's append gate
// (broadcastTaskManager.WaitVChannelsAcked) may be asked about a broadcast whose
// task was tombstoned and removed long ago -- a duplicate replica the primary
// wrote by retrying an append that had landed. "No task" alone cannot tell that
// apart from a broadcast that has not reached this cluster yet, which must keep
// waiting.
//
// A checker must answer true ONLY on a record written after the replica landed
// here, and must never forget one it answered true for while a replica that
// waits on it can still arrive. An error is transient: the gate keeps waiting and
// asks again.
type AppendFirstReplicaRecordedChecker = func(ctx context.Context, vchannel string) (bool, error)

var (
	appendFirstReplicaRecordedMu      sync.RWMutex
	appendFirstReplicaRecordedChecker AppendFirstReplicaRecordedChecker
)

// RegisterAppendFirstReplicaRecordedChecker registers the checker. The shard
// split's is DataCoord's record of a fenced split source.
func RegisterAppendFirstReplicaRecordedChecker(checker AppendFirstReplicaRecordedChecker) {
	appendFirstReplicaRecordedMu.Lock()
	defer appendFirstReplicaRecordedMu.Unlock()
	appendFirstReplicaRecordedChecker = checker
}

// IsAppendFirstReplicaRecorded asks the registered checker. With none registered
// -- a coord still starting, or one without the component that keeps the
// record -- nothing is on record.
func IsAppendFirstReplicaRecorded(ctx context.Context, vchannel string) (bool, error) {
	appendFirstReplicaRecordedMu.RLock()
	checker := appendFirstReplicaRecordedChecker
	appendFirstReplicaRecordedMu.RUnlock()
	if checker == nil {
		return false, nil
	}
	return checker(ctx, vchannel)
}

func resetAppendFirstReplicaRecordedChecker() {
	RegisterAppendFirstReplicaRecordedChecker(nil)
}
