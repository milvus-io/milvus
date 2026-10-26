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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PChannelIndexManager is the primary key index of one pchannel. It owns the
// Index of every vchannel of the pchannel and the lastObserved that all of
// them share.
type PChannelIndexManager struct {
	m        *Manager
	info     types.PChannelInfo
	onFatal  func(error)
	logger   *mlog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	lifetime *typeutil.Lifetime

	// lastObserved is the time tick through which the observe of the WAL
	// recovery wrote every change into the engines. Only ObserveTimeTick
	// moves it, at a persisted TimeTick or the recovery barrier, after the
	// writes up to it are applied. The handovers read it as covered, and
	// LastAcked follows it for a clean Index.
	lastObserved atomic.Uint64

	// opMu serializes Add, Remove and Close, so that the engine IO of an Add
	// happens outside mu.
	opMu sync.Mutex

	// mu guards indexes, the LastAcked heap, ackedMax, hadIndex, closed and
	// started.
	mu       sync.Mutex
	indexes  map[string]*index.Index
	acked    ackTracker
	ackedMax uint64
	// hadIndex is set by the first CreateCollection. Before it, LastAcked
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
		m:        m,
		info:     info,
		onFatal:  cfg.OnFatal,
		logger:   mlog.With(mlog.FieldModule("pkindex"), mlog.FieldPChannel(info.Name), mlog.Int64("term", info.Term)),
		ctx:      ctx,
		cancel:   cancel,
		lifetime: typeutil.NewLifetime(),
		indexes:  make(map[string]*index.Index),
	}
	c.lastObserved.Store(cfg.Seed)
	return c
}

// errClosed reports a call on a closed PChannelIndexManager or Index. what names it, such
// as "pchannel <name>". It wraps engine.ErrClosed, so the caller handles it
// like a closed engine.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of %s is closed", what), pkroot.ErrUnavailable)
}

// Start ends the recovery of the pchannel: every Index that CreateCollection
// opens from now on is ready at once. The recovery calls it once, after it
// installed and replayed the indexes of the existing collections.
func (c *PChannelIndexManager) Start() {
	c.mu.Lock()
	c.started = true
	c.mu.Unlock()
}

// CreateCollection opens the Index of a vchannel and returns it, with whether
// this call opened it. It returns the existing Index and false when the Index
// exists, so a caller that rolls back after a failure removes only an Index
// that it opened. It returns the error of the engine open.
//
// Before Start, the Index is not ready: the recovery of the pchannel marks
// it after the install of its committed table set. After Start, the Index is
// ready at once: a collection created after the recovery has a new collection
// ID in its vchannel name, so no manifest can exist for it.
func (c *PChannelIndexManager) CreateCollection(ctx context.Context, vchannel string, collectionID int64, pkDataType schemapb.DataType) (*index.Index, bool, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, false, errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()

	if idx := c.get(vchannel); idx != nil {
		return idx, false, nil
	}
	idx, err := index.Open(ctx, c.indexConfig(vchannel, collectionID, pkDataType))
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	c.indexes[vchannel] = idx
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

// DropCollection closes the Index of a vchannel, then removes the local
// directory of the vchannel. It removes the directory also when no Index is
// open. No open of the vchannel wipes the files later, because a vchannel name
// embeds the id of its collection. It is also the rollback of a
// CreateCollection whose append failed.
func (c *PChannelIndexManager) DropCollection(ctx context.Context, vchannel string) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.closeIndex(ctx, vchannel)
	dir := index.Dir(c.m.cfg.Root, vchannel)
	if err := os.RemoveAll(dir); err != nil {
		return pkroot.MarkIO(err, "remove dir %s", dir)
	}
	return nil
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

// detachLocked removes idx from the map and from the LastAcked heap. The
// caller holds mu.
func (c *PChannelIndexManager) detachLocked(idx *index.Index) {
	delete(c.indexes, idx.VChannel())
	c.acked.remove(idx)
}

// indexConfig is what an Index of this PChannelIndexManager needs, see index.Config.
func (c *PChannelIndexManager) indexConfig(vchannel string, collectionID int64, pkDataType schemapb.DataType) index.Config {
	cfg := c.m.cfg
	return index.Config{
		VChannel:        vchannel,
		CollectionID:    collectionID,
		PKDataType:      pkDataType,
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

// get returns the Index of a vchannel, nil when it does not exist.
func (c *PChannelIndexManager) get(vchannel string) *index.Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexes[vchannel]
}

// LastObserved returns lastObserved, see the field.
func (c *PChannelIndexManager) LastObserved() uint64 { return c.lastObserved.Load() }

// Apply writes w, the change of one WAL message, into the Index of vchannel
// and adds the Index to the LastAcked heap when the write made it dirty.
// The observe of the WAL recovery is the only caller: it calls Apply in
// time tick order, one message at a time, and ObserveTimeTick after the
// writes up to a persisted TimeTick. A vchannel without an Index is
// ignored: the collection has no index or was dropped. So is a write that
// finds the Index closed because a DropCollection detached it meanwhile.
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
			// the collection was dropped after the lookup: the write is
			// moot, and lastObserved must keep moving
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
// write up to tt. It never moves lastObserved back.
func (c *PChannelIndexManager) ObserveTimeTick(tt uint64) {
	for {
		cur := c.lastObserved.Load()
		if cur >= tt || c.lastObserved.CompareAndSwap(cur, tt) {
			break
		}
	}
}

// Close closes every Index. It keeps the local files.
func (c *PChannelIndexManager) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.cancel()
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
