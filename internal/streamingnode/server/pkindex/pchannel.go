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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PChannelIndexManager is the primary key index of one pchannel. It owns the Index of every
// vchannel of the pchannel, the watermark that all of them share, and the
// driver that starts their handovers.
type PChannelIndexManager struct {
	m        *Manager
	info     types.PChannelInfo
	wm       *watermark
	tail     func() WALTail
	onFatal  func(error)
	logger   *mlog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	lifetime *typeutil.Lifetime

	// driverDone is closed when the driver goroutine exits. It is nil when
	// the driver does not run.
	driverDone chan struct{}

	// opMu serializes Add, Remove and Close, so that the engine IO of an Add
	// happens outside mu.
	opMu sync.Mutex

	// mu guards indexes, the LastAcked heap, schedules, ackedMax and claimed.
	mu        sync.Mutex
	indexes   map[string]*index.Index
	acked     ackTracker
	schedules map[*index.Index]*handoverSchedule // the driver bookkeeping of each Index
	ackedMax  uint64
	// claimStarted is set by StartClaim. claimed is set when the pchannel is
	// claimed and the manifests of the recovered indexes are looked up. An
	// Index added after that is ready at once.
	claimStarted bool
	claimed      bool
}

func newPChannelIndexManager(m *Manager, info types.PChannelInfo, seed uint64, tail func() WALTail, onFatal func(error)) *PChannelIndexManager {
	ctx, cancel := context.WithCancel(context.Background())
	if tail == nil {
		tail = func() WALTail { return WALTail{} }
	}
	return &PChannelIndexManager{
		m:         m,
		info:      info,
		wm:        newWatermark(seed),
		tail:      tail,
		onFatal:   onFatal,
		logger:    mlog.With(mlog.FieldModule("pkindex"), mlog.FieldPChannel(info.Name), mlog.Int64("term", info.Term)),
		ctx:       ctx,
		cancel:    cancel,
		lifetime:  typeutil.NewLifetime(),
		indexes:   make(map[string]*index.Index),
		schedules: make(map[*index.Index]*handoverSchedule),
	}
}

// errClosed reports a call on a closed PChannelIndexManager or Index. what names it, such
// as "pchannel <name>". It wraps engine.ErrClosed, so the caller handles it
// like a closed engine.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of %s is closed", what), pkroot.ErrUnavailable)
}

// CreateCollection opens the Index of a vchannel and reports whether this
// call opened it. It returns false and no error when the Index exists, so a
// caller that rolls back after a failure removes only an Index that it
// opened. It returns the error of the engine open.
func (c *PChannelIndexManager) CreateCollection(ctx context.Context, vchannel string, collectionID int64, pkDataType schemapb.DataType) (created bool, err error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return false, errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()

	if c.get(vchannel) != nil {
		return false, nil
	}
	idx, err := index.Open(ctx, c.indexConfig(vchannel, collectionID, pkDataType))
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	c.indexes[vchannel] = idx
	c.schedules[idx] = &handoverSchedule{backoff: c.newBackoff()}
	ready := c.claimed
	c.mu.Unlock()
	if ready {
		// A collection created after the claim has a new collection ID in its
		// vchannel name, so no manifest can exist for it.
		idx.MarkReady(ctx, false)
	}
	return true, nil
}

// remove closes the Index of a vchannel. It waits for a running handover
// and for every open WriteGuard of the vchannel. It keeps the local files,
// like Close does for every vchannel. Only the tests call it, to replay the
// restart of one vchannel.
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

// attachedLocked reports whether idx is the open Index of its vchannel. The
// caller holds mu.
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
		OnWriterBegin: func() func() {
			token := c.wm.enter()
			return func() { c.wm.end(token) }
		},
		LastApplied: c.wm.lastApplied,
		OnDirtied:   c.trackDirty,
		OnRefresh:   c.refreshAcked,
	}
}

// Authority returns the write path authority of a vchannel, nil when the
// vchannel has no Index. The WAL interceptor decides every write through it.
func (c *PChannelIndexManager) Authority(vchannel string) *authority.Authority {
	idx := c.get(vchannel)
	if idx == nil {
		return nil
	}
	return idx.Authority()
}

// get returns the Index of a vchannel, nil when it does not exist.
func (c *PChannelIndexManager) get(vchannel string) *index.Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexes[vchannel]
}

// ObserveTimeTick tells the PChannelIndexManager that the TimeTick message with tt is appended.
func (c *PChannelIndexManager) ObserveTimeTick(tt uint64) {
	c.wm.tick(tt)
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
	indexes := make([]*index.Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		indexes = append(indexes, idx)
	}
	for _, idx := range indexes {
		c.detachLocked(idx)
	}
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
