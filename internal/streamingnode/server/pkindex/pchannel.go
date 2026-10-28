// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pkindex

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PChannelIndexManager is the primary key index of one pchannel. It owns the
// Index of every vchannel of the pchannel, the lastObserved that all of them
// share, and the driver that starts their handovers.
type PChannelIndexManager struct {
	m       *Manager
	info    types.PChannelInfo
	onFatal func(error)
	// vchannelState is PChannelConfig.VChannelState.
	vchannelState func(vchannel string) (VChannelState, bool)
	logger        *mlog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	lifetime      *typeutil.Lifetime

	// driverDone is closed when the driver goroutine exits. It is nil when
	// the driver does not run.
	driverDone chan struct{}

	// lastObserved is the time tick through which the observe of the WAL
	// recovery wrote every change into the engines. Only ObserveTimeTick
	// moves it, at a persisted TimeTick or the recovery barrier, after the
	// writes up to it are applied. The handovers read it as covered, and
	// LastAcked follows it for a clean Index.
	lastObserved atomic.Uint64
	// tail is the WAL tail that the last ObserveTimeTick saw. The driver
	// reads it instead of calling the recovery storage.
	tail atomic.Pointer[WALTailSnapshot]
	// stallSince is the lastObserved at which the index started to hold the
	// WAL checkpoint back, 0 while it does not. ObserveTimeTick keeps it.
	stallSince atomic.Uint64
	// wake wakes the driver: ObserveTimeTick sends when a trigger is met. It
	// has room for one signal. A second signal while the driver is busy is
	// not needed: the driver looks at every dirty Index when it wakes.
	wake chan struct{}
	// steps counts the driver steps, for the tests.
	steps atomic.Int64

	// opMu serializes OpenIndex, DropIndex and Close, so that the engine IO
	// of an open happens outside mu.
	opMu sync.Mutex

	// mu guards indexes, detached, the LastAcked heap, schedules, ackedMax,
	// hadIndex, closed and started.
	mu      sync.Mutex
	indexes map[string]*index.Index
	// detached are the indexes that DetachIndex removed from indexes and no
	// DropIndex closed yet. Close closes them.
	detached  map[*index.Index]struct{}
	acked     ackTracker
	schedules map[*index.Index]*handoverSchedule // the driver bookkeeping of each Index
	ackedMax  uint64
	// hadIndex is set by the first OpenIndex. Before it, LastAcked
	// has no index to report on.
	hadIndex bool
	// closed is set by Close, when the indexes are detached. LastAcked then
	// keeps the value of the close: the heap is empty, but the writes of the
	// closed indexes are not committed.
	closed bool
	// started is set by Start: the recovery of the pchannel is done. An Index
	// created after that is ready at once; one created before waits for the
	// recovery to mark it.
	started bool
}

func newPChannelIndexManager(m *Manager, cfg PChannelConfig) *PChannelIndexManager {
	ctx, cancel := context.WithCancel(context.Background())
	info := cfg.PChannel
	c := &PChannelIndexManager{
		m:             m,
		info:          info,
		onFatal:       cfg.OnFatal,
		vchannelState: cfg.VChannelState,
		logger:        mlog.With(mlog.FieldModule("pkindex"), mlog.FieldPChannel(info.Name), mlog.Int64("term", info.Term)),
		ctx:           ctx,
		cancel:        cancel,
		lifetime:      typeutil.NewLifetime(),
		indexes:       make(map[string]*index.Index),
		detached:      make(map[*index.Index]struct{}),
		schedules:     make(map[*index.Index]*handoverSchedule),
		wake:          make(chan struct{}, 1),
	}
	c.lastObserved.Store(cfg.Seed)
	c.tail.Store(&WALTailSnapshot{})
	return c
}

// ErrOtherIndexOpen reports an OpenIndex of a vchannel that has an open
// Index of another index ID.
var ErrOtherIndexOpen = errors.New("another primary key index of the vchannel is open")

// errClosed reports a call on a closed PChannelIndexManager or Index. what names it, such
// as "pchannel <name>". It wraps engine.ErrClosed, so the caller handles it
// like a closed engine.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of %s is closed", what), pkroot.ErrUnavailable)
}

// Start ends the recovery of the pchannel: it starts the handover driver,
// and every Index that OpenIndex opens from now on is ready at once.
// The recovery calls it once, after it installed and replayed the indexes of
// the existing collections. No handover runs before it.
func (c *PChannelIndexManager) Start() {
	c.start(true)
}

// start is Start with the choice to run the driver goroutine. The tests do
// not run it and call step by hand.
func (c *PChannelIndexManager) start(driver bool) {
	c.mu.Lock()
	already := c.started
	c.started = true
	c.mu.Unlock()
	if already || !driver {
		return
	}
	c.startDriver()
}

// OpenIndex opens the Index of spec.VChannel and returns it, with whether
// this call opened it. It returns the open Index and false when the
// vchannel has an open Index of spec.IndexID. An open Index of another index
// ID is an error: the caller detaches it first, see DetachIndex. It returns
// the error of the engine open.
//
// Before Start, the Index is not ready: the recovery of the pchannel marks
// it after the install of its committed table set. After Start, the Index is
// ready at once: an index enabled after the recovery has a new index ID, so
// no manifest can exist for it.
func (c *PChannelIndexManager) OpenIndex(ctx context.Context, spec IndexSpec) (*index.Index, bool, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, false, errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()

	if idx := c.get(spec.VChannel); idx != nil {
		if idx.IndexID() != spec.IndexID {
			return nil, false, errors.Wrapf(ErrOtherIndexOpen, "vchannel %s: index %d is open, open index %d", spec.VChannel, idx.IndexID(), spec.IndexID)
		}
		return idx, false, nil
	}
	idx, err := index.Open(ctx, c.indexConfig(spec))
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	c.indexes[spec.VChannel] = idx
	c.schedules[idx] = &handoverSchedule{backoff: c.newBackoff()}
	c.hadIndex = true
	ready := c.started
	c.mu.Unlock()
	if ready {
		idx.MarkReady(ctx)
	}
	return idx, true, nil
}

// remove closes the Index of a vchannel. It waits for a running handover.
// It keeps the local files, like Close does for every vchannel. Only the
// tests call it, to replay the restart of one vchannel.
func (c *PChannelIndexManager) remove(ctx context.Context, vchannel string) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.closeIndex(ctx, vchannel)
}

// DetachIndex removes the Index of a vchannel from the open indexes when its
// index ID is indexID, and reports whether it did. indexID 0 matches any
// Index. The Index stays open until DropIndex of the same vchannel and index
// ID, or Close, closes it: the close waits for a running handover, so the
// observe of the WAL recovery detaches at once and drops in a task. After
// the detach the vchannel has no Decider, its writes are not applied, and a
// new Index of the vchannel can open.
func (c *PChannelIndexManager) DetachIndex(vchannel string, indexID int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx, ok := c.indexes[vchannel]
	if !ok || !matchesIndex(idx, indexID) {
		return false
	}
	c.detachLocked(idx)
	c.detached[idx] = struct{}{}
	return true
}

// DropIndex closes the Index of a vchannel whose index ID is indexID, open or
// detached, and removes its local directory. indexID 0 matches any Index of
// the vchannel and removes the local directory of the vchannel: the drop of
// its collection. It removes the directory also when no Index is open. No
// open of the same index ID wipes the files later, because the IDs of
// indexes are not reused.
func (c *PChannelIndexManager) DropIndex(ctx context.Context, vchannel string, indexID int64) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()

	var closing []*index.Index
	c.mu.Lock()
	if idx, ok := c.indexes[vchannel]; ok && matchesIndex(idx, indexID) {
		c.detachLocked(idx)
		closing = append(closing, idx)
	}
	for idx := range c.detached {
		if idx.VChannel() == vchannel && matchesIndex(idx, indexID) {
			delete(c.detached, idx)
			closing = append(closing, idx)
		}
	}
	c.mu.Unlock()
	for _, idx := range closing {
		idx.Close(ctx)
	}
	dir := index.VChannelDir(c.m.cfg.Root, vchannel)
	if indexID != 0 {
		dir = index.Dir(c.m.cfg.Root, vchannel, indexID)
	}
	if err := os.RemoveAll(dir); err != nil {
		return pkroot.MarkIO(err, "remove dir %s", dir)
	}
	return nil
}

// matchesIndex reports whether idx has the index ID indexID, or indexID is 0.
func matchesIndex(idx *index.Index, indexID int64) bool {
	return indexID == 0 || idx.IndexID() == indexID
}

// VChannelState returns what the observe of the WAL recovery knows about a
// vchannel now, see PChannelConfig.VChannelState.
func (c *PChannelIndexManager) VChannelState(vchannel string) (VChannelState, bool) {
	if c.vchannelState == nil {
		return VChannelState{}, false
	}
	return c.vchannelState(vchannel)
}

// closeIndex detaches and closes the Index of a vchannel, if it is open. The
// caller holds opMu.
func (c *PChannelIndexManager) closeIndex(ctx context.Context, vchannel string) {
	c.mu.Lock()
	idx, ok := c.indexes[vchannel]
	if ok {
		c.detachLocked(idx)
	}
	c.mu.Unlock()
	if ok {
		idx.Close(ctx)
	}
}

// attached reports whether idx is the open Index of its vchannel.
func (c *PChannelIndexManager) attached(idx *index.Index) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attachedLocked(idx)
}

// attachedLocked is attached for a caller that holds mu.
func (c *PChannelIndexManager) attachedLocked(idx *index.Index) bool {
	return c.indexes[idx.VChannel()] == idx
}

// detachLocked removes idx from the map, from the LastAcked heap and from the
// driver schedules. The caller holds mu.
func (c *PChannelIndexManager) detachLocked(idx *index.Index) {
	delete(c.indexes, idx.VChannel())
	c.acked.remove(idx)
	delete(c.schedules, idx)
}

// indexConfig is what an Index of this PChannelIndexManager needs, see index.Config.
func (c *PChannelIndexManager) indexConfig(spec IndexSpec) index.Config {
	cfg := c.m.cfg
	return index.Config{
		VChannel:        spec.VChannel,
		CollectionID:    spec.CollectionID,
		IndexID:         spec.IndexID,
		EnableTimeTick:  spec.EnableTimeTick,
		Term:            c.info.Term,
		Ctx:             c.ctx,
		Logger:          c.logger,
		Root:            cfg.Root,
		StorageRootPath: cfg.StorageRootPath,
		Shared:          cfg.Shared,
		AllocID:         cfg.AllocID,
		Store:           cfg.Store,
		Committer:       cfg.Committer,
		Params:          func() index.Params { return cfg.Params().Params },
		LastObserved:    c.LastObserved,
		OnRefresh:       c.refreshAcked,
	}
}

// Decider returns the write path decider of a vchannel, nil when the
// vchannel has no Index. The WAL interceptor decides every write through it.
func (c *PChannelIndexManager) Decider(vchannel string) dedup.Decider {
	idx := c.get(vchannel)
	if idx == nil {
		return nil
	}
	return idx.Decider()
}

// IndexOf returns the open Index of a vchannel, nil when it has none.
func (c *PChannelIndexManager) IndexOf(vchannel string) *index.Index {
	return c.get(vchannel)
}

// get returns the Index of a vchannel, nil when it does not exist.
func (c *PChannelIndexManager) get(vchannel string) *index.Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexes[vchannel]
}

// LastObserved returns lastObserved, see the field.
func (c *PChannelIndexManager) LastObserved() uint64 { return c.lastObserved.Load() }

// Closed reports whether Close ran, also for a PChannelIndexManager that
// Manager.New returned closed because the Manager had stopped.
func (c *PChannelIndexManager) Closed() bool { return c.ctx.Err() != nil }

// Info returns the pchannel of the PChannelIndexManager.
func (c *PChannelIndexManager) Info() types.PChannelInfo { return c.info }

// Apply writes w, the change of one WAL message, into the Index of vchannel
// and adds the Index to the LastAcked heap when the write made it dirty.
// The observe of the WAL recovery is the only caller: it calls Apply in
// time tick order, one message at a time, and ObserveTimeTick after the
// writes up to a persisted TimeTick. A vchannel without an Index is
// ignored: the vchannel has no enabled index, or its index or collection was
// dropped. So is a write that finds the Index closed because a drop detached
// it meanwhile.
// Every other error of the engine write is returned as it is.
func (c *PChannelIndexManager) Apply(ctx context.Context, vchannel string, w index.Write) error {
	idx := c.get(vchannel)
	if idx == nil {
		return nil
	}
	return c.apply(ctx, idx, w)
}

// apply is Apply on a looked-up Index.
func (c *PChannelIndexManager) apply(ctx context.Context, idx *index.Index, w index.Write) error {
	dirtied, err := idx.Apply(ctx, w)
	if err != nil {
		if errors.Is(err, engine.ErrClosed) && !c.attached(idx) {
			// the index was dropped after the lookup: the write is moot,
			// and lastObserved must keep moving
			return nil
		}
		return err
	}
	if dirtied {
		c.trackDirty(idx)
	}
	return nil
}

// ObserveTimeTick records that the observe of the WAL recovery applied every
// write up to tt, with the WAL tail as the recovery storage sees it now. It
// never moves lastObserved back. It passes tt to the Decider of every index,
// also an idle one, which removes the in-doubt in-flight entries up to tt.
// Then it evaluates the handover triggers and wakes the driver when one is
// met. The observe calls it under its own
// lock, so the evaluation only reads: the dirty set, the stats of the
// engines, and atomics.
func (c *PChannelIndexManager) ObserveTimeTick(tt uint64, tail WALTailSnapshot) {
	for {
		cur := c.lastObserved.Load()
		if cur >= tt || c.lastObserved.CompareAndSwap(cur, tt) {
			break
		}
	}
	for _, idx := range c.list() {
		idx.Decider().Observe(tt, nil)
	}
	c.tail.Store(&tail)
	c.trackStall(tt, tail)
	if c.wantsHandover() {
		c.signal()
	}
}

// trackStall starts the stall clock at tt when the index holds the WAL
// checkpoint back, and clears it when it does not.
func (c *PChannelIndexManager) trackStall(tt uint64, tail WALTailSnapshot) {
	if !c.holdsCheckpointBack(tail) {
		c.stallSince.Store(0)
		return
	}
	c.stallSince.CompareAndSwap(0, tt)
}

// holdsCheckpointBack reports whether a dirty Index keeps the WAL checkpoint
// below the completed point of the ack tracker. It uses LastAcked without
// the clamp: the clamp can hide the Index that holds it back.
func (c *PChannelIndexManager) holdsCheckpointBack(tail WALTailSnapshot) bool {
	return len(c.dirtyIndexes()) > 0 && c.lastAckedUnclamped() < tail.CompletedTimeTick
}

// signal wakes the driver, unless a signal waits already.
func (c *PChannelIndexManager) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// ObserveFlush tells the Index of a vchannel that the observe saw a flush at
// tt. The Index then commits a checkpoint at or above tt soon, once
// lastObserved reaches tt, regardless of size and tail. The observe of the WAL
// recovery calls it when it sees a ManualFlush, with the time tick of the
// flush. The request stays until a handover commits a checkpoint at or above
// tt. A vchannel without an Index is ignored.
func (c *PChannelIndexManager) ObserveFlush(vchannel string, tt uint64) {
	if idx := c.get(vchannel); idx != nil {
		c.observeFlush(idx, tt)
	}
}

// ObserveFlushAll is ObserveFlush for every vchannel: a FlushAll.
func (c *PChannelIndexManager) ObserveFlushAll(tt uint64) {
	for _, idx := range c.list() {
		c.observeFlush(idx, tt)
	}
}

// observeFlush raises the flush time tick of idx to tt.
func (c *PChannelIndexManager) observeFlush(idx *index.Index, tt uint64) {
	s := c.scheduleOf(idx)
	for {
		cur := s.flushTT.Load()
		if cur >= tt || s.flushTT.CompareAndSwap(cur, tt) {
			return
		}
	}
}

// Close stops the driver and closes every Index. It keeps the local files.
func (c *PChannelIndexManager) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.cancel()
	if c.driverDone != nil {
		<-c.driverDone
	}
	c.lifetime.Wait()

	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	// LastAcked keeps the value of this moment, also when nothing asked for
	// it before: the heap is about to go
	c.ackedMax = max(c.ackedMax, c.lastAckedLocked())
	indexes := make([]*index.Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		indexes = append(indexes, idx)
	}
	for _, idx := range indexes {
		c.detachLocked(idx)
	}
	for idx := range c.detached {
		indexes = append(indexes, idx)
	}
	c.detached = make(map[*index.Index]struct{})
	c.closed = true
	c.mu.Unlock()
	for _, idx := range indexes {
		idx.Close(c.ctx)
	}
	c.m.forget(c)
	c.logger.Info(c.ctx, "primary key index channel closed", mlog.Int("vchannels", len(indexes)))
}

// fatal reports an error that the WAL instance cannot recover from.
func (c *PChannelIndexManager) fatal(err error) {
	if c.onFatal != nil {
		c.onFatal(err)
	}
}

// list returns the open indexes.
func (c *PChannelIndexManager) list() []*index.Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*index.Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		out = append(out, idx)
	}
	return out
}
