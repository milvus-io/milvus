// Package autowalimpls joins physical logs at durable AlterWAL boundaries.
package autowalimpls

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// OpenBackend opens a physical handle. Historical readers must use AccessModeRO.
type OpenBackend func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error)

// WAL preserves one logical lifetime while switching the current physical log.
// Append exclusion is supplied by the logical WAL. mu protects handle discovery
// and readers; it is not a replacement for the logical PChannel append lock.
type WAL struct {
	mu             sync.Mutex
	channel        types.PChannelInfo
	open           OpenBackend
	current        walimpls.WALImpls
	writers        map[message.WALName]walimpls.WALImpls
	readers        map[message.WALName]walimpls.WALImpls
	lastSwitchTick uint64
	lastSwitchID   message.MessageID
	closed         bool
}

// New takes ownership of the final writer and any physical writers acquired
// during recovery. It performs no recovery, I/O or writer reopening.
func New(initial walimpls.WALImpls, open OpenBackend, recovered ...walimpls.WALImpls) *WAL {
	w := &WAL{
		channel: initial.Channel(), open: open, current: initial,
		writers: make(map[message.WALName]walimpls.WALImpls), readers: make(map[message.WALName]walimpls.WALImpls),
	}
	if initial.Channel().AccessMode == types.AccessModeRW {
		w.writers[initial.WALName()] = initial
	} else {
		w.readers[initial.WALName()] = initial
	}
	for _, backend := range recovered {
		w.writers[backend.WALName()] = backend
	}
	return w
}

func (w *WAL) Channel() types.PChannelInfo { return w.channel }
func (w *WAL) WALName() message.WALName    { w.mu.Lock(); defer w.mu.Unlock(); return w.current.WALName() }

func (w *WAL) backend(ctx context.Context, name message.WALName, write bool) (walimpls.WALImpls, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, merr.WrapErrServiceUnavailableMsg("auto WAL closed")
	}
	if b := w.writers[name]; b != nil {
		return b, nil
	}
	if !write {
		if b := w.readers[name]; b != nil {
			return b, nil
		}
	}
	mode := types.AccessModeRO
	if write {
		mode = types.AccessModeRW
	}
	b, err := w.open(ctx, name, mode)
	if err != nil {
		return nil, err
	}
	if write {
		w.writers[name] = b
	} else {
		w.readers[name] = b
	}
	return b, nil
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
		return current.Append(ctx, msg)
	}
	target := message.WALName(message.MustAsMutableAlterWALMessageV2(msg).Header().TargetWalName)
	next, err := w.backend(ctx, target, true)
	if err != nil {
		return nil, err
	}
	start, err := message.WALSwitchStart(msg)
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
		start, err = next.Append(ctx, anchor)
		if err != nil {
			return nil, err
		}
		message.SetWALSwitchStart(msg, start)
	}
	id, err := current.Append(ctx, msg)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.current, w.lastSwitchTick, w.lastSwitchID = next, msg.TimeTick(), id
	w.mu.Unlock()
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
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	for _, b := range w.readers {
		b.Close()
	}
	for _, b := range w.writers {
		b.Close()
	}
}
