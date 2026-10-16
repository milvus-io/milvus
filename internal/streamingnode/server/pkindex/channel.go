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
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Channel is the primary key index of one pchannel. It owns the Index of every
// vchannel of the pchannel, the watermark that all of them share, and the
// driver that starts their handovers.
type Channel struct {
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

	// mu guards indexes, the LastAcked heap, ackedMax, claimed, and
	// Index.removed and Index.recovered.
	mu       sync.Mutex
	indexes  map[string]*Index
	acked    ackTracker
	ackedMax uint64
	// claimStarted is set by StartClaim. claimed is set when the pchannel is
	// claimed and the manifests of the recovered indexes are looked up. An
	// Index added after that is ready at once.
	claimStarted bool
	claimed      bool
}

func newChannel(m *Manager, info types.PChannelInfo, seed uint64, tail func() WALTail, onFatal func(error)) *Channel {
	ctx, cancel := context.WithCancel(context.Background())
	if tail == nil {
		tail = func() WALTail { return WALTail{} }
	}
	return &Channel{
		m:        m,
		info:     info,
		wm:       newWatermark(seed),
		tail:     tail,
		onFatal:  onFatal,
		logger:   mlog.With(mlog.FieldModule("pkindex"), mlog.FieldPChannel(info.Name), mlog.Int64("term", info.Term)),
		ctx:      ctx,
		cancel:   cancel,
		lifetime: typeutil.NewLifetime(),
		indexes:  make(map[string]*Index),
	}
}

// errClosed reports a call on a closed Channel or Index. what names it, such
// as "pchannel <name>". It wraps engine.ErrClosed, so the caller handles it
// like a closed engine.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of %s is closed", what), pkroot.ErrUnavailable)
}

// Add opens the Index of a vchannel and returns it, with whether this call
// opened it. It returns the existing Index and false when the Index exists,
// so a caller that rolls back after a failure removes only an Index that it
// opened. It returns the error of the engine open.
func (c *Channel) Add(ctx context.Context, vchannel string, collectionID int64, pkDataType schemapb.DataType) (*Index, bool, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, false, errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()

	if idx := c.Get(vchannel); idx != nil {
		return idx, false, nil
	}
	idx, err := openIndex(ctx, c, vchannel, collectionID, pkDataType)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	c.indexes[vchannel] = idx
	ready := c.claimed
	c.mu.Unlock()
	if ready {
		// A collection created after the claim has a new collection ID in its
		// vchannel name, so no manifest can exist for it.
		idx.markReady(ctx, false)
	}
	return idx, true, nil
}

// Remove closes the Index of a vchannel. It waits for a running handover
// and for every open WriteGuard of the vchannel. It keeps the local files.
func (c *Channel) Remove(ctx context.Context, vchannel string) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.closeIndex(ctx, vchannel)
}

// Drop closes the Index of a vchannel like Remove, then removes the local
// directory of the vchannel. It removes the directory also when no Index is
// open. No open of the vchannel wipes the files later, because a vchannel name
// embeds the id of its collection.
func (c *Channel) Drop(ctx context.Context, vchannel string) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.closeIndex(ctx, vchannel)
	dir := indexDir(c.m.cfg.Root, vchannel)
	if err := os.RemoveAll(dir); err != nil {
		return pkroot.MarkIO(err, "remove dir %s", dir)
	}
	return nil
}

// closeIndex detaches and closes the Index of a vchannel, if it is open. The
// caller holds opMu.
func (c *Channel) closeIndex(ctx context.Context, vchannel string) {
	c.mu.Lock()
	idx, ok := c.indexes[vchannel]
	if ok {
		c.detachLocked(idx)
	}
	c.mu.Unlock()
	if ok {
		idx.close(ctx)
	}
}

// detachLocked removes idx from the map and from the LastAcked heap. The
// caller holds mu.
func (c *Channel) detachLocked(idx *Index) {
	delete(c.indexes, idx.vchannel)
	idx.removed = true
	c.acked.remove(idx)
}

// Get returns the Index of a vchannel, nil when it does not exist.
func (c *Channel) Get(vchannel string) *Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexes[vchannel]
}

// Tick tells the Channel that the TimeTick message with tt is appended.
func (c *Channel) Tick(tt uint64) {
	c.wm.tick(tt)
}

// Close stops the driver and closes every Index. It keeps the local files.
func (c *Channel) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.cancel()
	if c.driverDone != nil {
		<-c.driverDone
	}
	c.lifetime.Wait()

	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	indexes := make([]*Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		indexes = append(indexes, idx)
	}
	for _, idx := range indexes {
		c.detachLocked(idx)
	}
	c.mu.Unlock()
	for _, idx := range indexes {
		idx.close(c.ctx)
	}
	c.m.forget(c)
	c.logger.Info(c.ctx, "primary key index channel closed", mlog.Int("vchannels", len(indexes)))
}

// fatal reports an error that the WAL instance cannot recover from.
func (c *Channel) fatal(err error) {
	if c.onFatal != nil {
		c.onFatal(err)
	}
}

// list returns the open indexes.
func (c *Channel) list() []*Index {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		out = append(out, idx)
	}
	return out
}
