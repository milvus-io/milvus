// Package autowalimpls joins physical logs at durable AlterWAL boundaries.
package autowalimpls

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// OpenBackend opens a physical handle. Opening for historical reads uses
// AccessModeRO; an already-owned RW handle may serve those reads directly.
// Physical RO handles do not own opener resources or scanners, so replacing
// and closing an RO handle must leave their lifetimes independent.
type OpenBackend func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error)

// WAL preserves one logical lifetime while switching the current physical log.
// Append exclusion is supplied by the logical WAL. mu protects handle discovery
// and readers; it is not a replacement for the logical PChannel append lock.
type WAL struct {
	mu             sync.Mutex
	openGate       chan struct{} // serializes physical opens with cancellable waiting
	channel        types.PChannelInfo
	open           OpenBackend
	current        walimpls.WALImpls                     // the only handle used for current reads and writes
	readers        map[message.WALName]walimpls.WALImpls // one historical handle per backend; excludes current
	lastSwitchTick uint64
	lastSwitchID   message.MessageID
	closed         bool
}

// New takes ownership of the initial physical handle. It performs no recovery,
// I/O or writer reopening.
func New(initial walimpls.WALImpls, open OpenBackend) *WAL {
	return &WAL{
		channel: initial.Channel(), open: open, current: initial,
		openGate: make(chan struct{}, 1),
		readers:  make(map[message.WALName]walimpls.WALImpls),
	}
}

func (w *WAL) Channel() types.PChannelInfo { return w.channel }
func (w *WAL) WALName() message.WALName    { w.mu.Lock(); defer w.mu.Unlock(); return w.current.WALName() }

// cachedBackend requires mu. Retired RW handles can serve reads and be reused
// for a later switch; an RO handle cannot be used for writes.
func (w *WAL) cachedBackend(name message.WALName, write bool) walimpls.WALImpls {
	if b := w.current; b.WALName() == name && (!write || b.Channel().AccessMode == types.AccessModeRW) {
		return b
	}
	if b := w.readers[name]; b != nil && (!write || b.Channel().AccessMode == types.AccessModeRW) {
		return b
	}
	return nil
}

func (w *WAL) backend(ctx context.Context, name message.WALName, write bool) (walimpls.WALImpls, error) {
	w.mu.Lock()
	b, closed := w.cachedBackend(name, write), w.closed
	w.mu.Unlock()
	if closed {
		return nil, merr.WrapErrServiceUnavailableMsg("auto WAL closed")
	}
	if b != nil {
		return b, nil
	}
	// Concurrent RW opens can fence each other, so do not open duplicate
	// writers and discard a loser. Cached appends never acquire openGate.
	select {
	case w.openGate <- struct{}{}:
		defer func() { <-w.openGate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	w.mu.Lock()
	b, closed = w.cachedBackend(name, write), w.closed
	w.mu.Unlock()
	if closed {
		return nil, merr.WrapErrServiceUnavailableMsg("auto WAL closed")
	}
	if b != nil {
		return b, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mode := types.AccessModeRO
	if write {
		mode = types.AccessModeRW
	}
	b, err := w.open(ctx, name, mode)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		b.Close()
		return nil, merr.WrapErrServiceUnavailableMsg("auto WAL closed during backend open")
	}
	// A prepared RW handle becomes current only after the durable boundary.
	previous := w.readers[name]
	w.readers[name] = b
	w.mu.Unlock()
	if previous != nil {
		// Only an RO handle can be replaced. Physical RO Close leaves shared
		// opener resources and independently owned scanners intact.
		previous.Close()
	}
	return b, nil
}

// selectWriter requires mu and a handle acquired through backend. Only the
// append path or startup recovery selects a writer; historical readers do not.
func (w *WAL) selectWriter(next walimpls.WALImpls) {
	if w.current == next {
		return
	}
	w.readers[w.current.WALName()] = w.current
	delete(w.readers, next.WALName())
	w.current = next
}

// SwitchWriterForRecovery selects the writer reached by replaying a durable
// AlterWAL. Call only during startup, before admitting appends. It acquires the
// target writer (and its fence) before the caller allocates a new RecoveryBarrier.
// It writes no anchor or AlterWAL; existing scanners keep their own read position.
func (w *WAL) SwitchWriterForRecovery(ctx context.Context, name message.WALName) error {
	target, err := w.backend(ctx, name, true)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.selectWriter(target)
	w.mu.Unlock()
	return nil
}

func (w *WAL) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	w.mu.Lock()
	current := w.current
	if msg.MessageType() == message.MessageTypeAlterWAL && w.lastSwitchTick == msg.TimeTick() && w.lastSwitchID != nil {
		id := w.lastSwitchID
		w.mu.Unlock()
		return id, nil
	}
	w.mu.Unlock()
	if msg.MessageType() != message.MessageTypeAlterWAL {
		return appendWithRetry(ctx, current, msg)
	}
	target := message.WALName(message.MustAsMutableAlterWALMessageV2(msg).Header().TargetWalName)
	start, err := message.WALSwitchStart(msg)
	if err != nil {
		return nil, err
	}
	var next walimpls.WALImpls
	err = retry(ctx, w.channel.Name, msg, func() error {
		var err error
		next, err = w.backend(ctx, target, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	if start == nil && target == current.WALName() {
		// A redundant request confirms the existing prefix without creating a link.
		raw, _ := msg.Properties().Get("_lc")
		start, err = message.UnmarshalMessageID(&commonpb.MessageID{WALName: commonpb.WALName(target), Id: raw})
		if err != nil {
			return nil, err
		}
		message.SetWALSwitchStart(msg, start)
	}
	if start == nil {
		// This persisted anchor also makes an otherwise empty target addressable.
		anchor := message.NewTimeTickMessageBuilderV1().WithHeader(&message.TimeTickMessageHeader{}).
			WithBody(&msgpb.TimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_TimeTick, Timestamp: msg.TimeTick()}}).WithAllVChannel().MustBuildMutable().
			WithTimeTick(msg.TimeTick()).WithLastConfirmedUseMessageID().WithWALTerm(w.channel.Term)
		start, err = appendWithRetry(ctx, next, anchor)
		if err != nil {
			return nil, err
		}
		message.SetWALSwitchStart(msg, start)
	}
	id, err := appendWithRetry(ctx, current, msg)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.selectWriter(next)
	w.lastSwitchTick, w.lastSwitchID = msg.TimeTick(), id
	w.mu.Unlock()
	mlog.Info(ctx, "WAL writer switched", mlog.FieldPChannel(w.channel.Name), mlog.Stringer("oldWAL", current.WALName()), mlog.Stringer("targetWAL", target), mlog.Uint64("timetick", msg.TimeTick()))
	return id, nil
}

func (w *WAL) Truncate(ctx context.Context, id message.MessageID) error {
	b, err := w.backend(ctx, id.WALName(), false)
	if err != nil {
		return err
	}
	// Historical read handles must not acquire a writer just to perform GC.
	// Retention remains responsible for those logs until an owned handle exists.
	if b.Channel().AccessMode != types.AccessModeRW {
		return nil
	}
	return b.Truncate(ctx, id)
}

func (w *WAL) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	handles := []walimpls.WALImpls{w.current}
	for _, reader := range w.readers {
		handles = append(handles, reader)
	}
	w.mu.Unlock()
	// Snapshot ownership under mu: a switch can move handles between roles,
	// but no new handle can be installed after closed is set.
	for _, handle := range handles {
		handle.Close()
	}
}
