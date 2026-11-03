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

package index

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ Index = (*indexImpl)(nil)

// indexImpl is the primary key index of one vchannel on a streaming node.
type indexImpl struct {
	cfg          OpenConfig
	vchannel     string
	collectionID int64
	indexID      int64
	committedDir string
	eng          *engine.Engine
	auth         dedup.Decider
	logger       *mlog.Logger

	// ctx is canceled when the Index closes.
	ctx      context.Context
	cancel   context.CancelFunc
	lifetime *typeutil.Lifetime

	// ready is closed when the committed table set of the vchannel is
	// installed. Before that the writes wait for it.
	ready     chan struct{}
	readyOnce sync.Once
	closeOnce sync.Once

	lastCommitted atomic.Uint64
	// dirtyFlag is set by every successful engine write and cleared right
	// before a handover rotates.
	dirtyFlag atomic.Bool
	// pending is the prepared handover that is not committed yet.
	pending atomic.Pointer[pendingHandover]
	// installMu serializes the installs of committed table sets.
	installMu        sync.Mutex
	installedVersion int64
	installedPaths   map[sst.ID]string
	// recovered is the manifest of the vchannel at the open, set by
	// SetCommitted and installed by Install. Nil when the vchannel had none,
	// or after a TruncateUntil.
	recovered *manifest.View
	// resetEpoch counts the truncates. A handover prepared before a truncate
	// does not install its result. installMu guards it.
	resetEpoch uint64
	// resetTT is the time tick of the last truncate, 0 when none. Until a
	// handover commits a checkpoint at or above it, the next handover
	// replaces the committed table set (see manifest.FlushArgs.ResetTimeTick).
	resetTT atomic.Uint64
	// corrupted is set by Apply or TruncateUntil after a corruption error.
	// Close then removes the local directory, so the next open starts from
	// the manifest.
	corrupted atomic.Bool

	// failWrites, when set, makes every engine write of Apply fail with it.
	// Only the tests set it, see FailWritesForTest.
	failWrites atomic.Pointer[error]
	// onWrite, when set, runs before every engine write of Apply with the
	// batch. Only the tests set it, see OnWriteForTest.
	onWrite atomic.Pointer[func([]engine.Mutation)]
}

// Open opens the engine of a vchannel and builds the Index on top of it. The
// recovery of the pchannel marks it ready after the manifest lookup or the
// install.
//
// The engine directory of the vchannel may hold files from before this open:
// a crash, or a close that kept them. They are removed and the Index starts
// empty; the recovery replays the WAL from its checkpoint. The committed
// directory is kept: the install of the committed table set checks each file
// against the manifest and downloads what is missing or differs, so a
// restart does not download the tables it already has.
func Open(ctx context.Context, cfg OpenConfig) (Index, error) {
	params := cfg.Params()
	engineDir := filepath.Join(cfg.Dir, "engine")
	committedDir := filepath.Join(cfg.Dir, "committed")
	leftover, err := hasLeftover(engineDir)
	if err != nil {
		return nil, err
	}
	if leftover {
		if err := wipeLocal(engineDir); err != nil {
			return nil, err
		}
		cfg.Logger.Info(ctx, "removed the local primary key index engine from before this open, the index starts empty",
			mlog.FieldVChannel(cfg.VChannel))
	}
	eng, err := engine.Open(ctx, engine.Config{
		Dir:          engineDir,
		VChannel:     cfg.VChannel,
		Shared:       cfg.Engine,
		AllocID:      cfg.AllocID,
		MemTableSize: params.MemTableSize,
	})
	if err != nil {
		return nil, err
	}
	idxCtx, cancel := context.WithCancel(context.Background())
	idx := &indexImpl{
		cfg:          cfg,
		vchannel:     cfg.VChannel,
		collectionID: cfg.CollectionID,
		indexID:      cfg.IndexID,
		committedDir: committedDir,
		eng:          eng,
		logger:       cfg.Logger.With(mlog.FieldVChannel(cfg.VChannel), mlog.FieldCollectionID(cfg.CollectionID), mlog.Int64("indexID", cfg.IndexID)),
		ctx:          idxCtx,
		cancel:       cancel,
		lifetime:     typeutil.NewLifetime(),
		ready:        make(chan struct{}),
	}
	// the decider only reads the engine: the observe of the WAL recovery
	// writes it, through Apply
	idx.auth = dedup.New(cfg.VChannel, eng, dedup.Config{Ready: idx.ready})
	return idx, nil
}

// VChannel returns the vchannel of the Index.
func (idx *indexImpl) VChannel() string { return idx.vchannel }

// CollectionID returns the collection of the Index.
func (idx *indexImpl) CollectionID() int64 { return idx.collectionID }

// IndexID returns the ID of the primary key index of the Index.
func (idx *indexImpl) IndexID() int64 { return idx.indexID }

// Logger returns the logger of the Index, with its vchannel and collection.
func (idx *indexImpl) Logger() *mlog.Logger { return idx.logger }

// VChannelDir is the local directory of all Indexes of a vchannel under root.
func VChannelDir(root, vchannel string) string {
	return filepath.Join(root, vchannel)
}

// Dir is the local directory of the Index of the index indexID on a vchannel
// under root. It holds the engine directory and the committed directory.
func Dir(root, vchannel string, indexID int64) string {
	return filepath.Join(VChannelDir(root, vchannel), strconv.FormatInt(indexID, 10))
}

// hasLeftover reports whether any of dirs exists and is not empty.
func hasLeftover(dirs ...string) (bool, error) {
	for _, d := range dirs {
		ents, err := os.ReadDir(d)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false, pkroot.MarkIO(err, "list dir %s", d)
		}
		if len(ents) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// wipeLocal removes the local directories of an Index.
func wipeLocal(dirs ...string) error {
	for _, d := range dirs {
		if err := os.RemoveAll(d); err != nil {
			return pkroot.MarkIO(err, "remove dir %s", d)
		}
	}
	return nil
}

// markDirty sets the dirty flag and reports whether it was clear before: the
// first write since the last rotate. The caller adds the Index to the
// LastAcked heap.
func (idx *indexImpl) markDirty() bool {
	return idx.dirtyFlag.CompareAndSwap(false, true)
}

// dirty reports whether the Index has writes that are not committed: writes
// since the last rotate, frozen generations, or a pending handover.
func (idx *indexImpl) dirty() bool {
	return idx.dirtyFlag.Load() || idx.pending.Load() != nil || len(idx.eng.FrozenGenerations()) > 0
}

// HandoverState implements Index.
func (idx *indexImpl) HandoverState() HandoverState {
	return HandoverState{
		Dirty:                 idx.dirty(),
		Pending:               idx.pending.Load() != nil,
		ActiveBytes:           idx.eng.Stats().ActiveBytes,
		LastCommittedTimeTick: idx.lastCommitted.Load(),
	}
}

// Decider returns the write path decider of the vchannel. The WAL
// interceptor decides every write through it.
func (idx *indexImpl) Decider() dedup.Decider {
	return idx.auth
}

// Close stops the Index: it waits for a running handover, then closes the
// engine. It keeps the local files, except after an Apply or a TruncateUntil
// saw a corruption error: then it removes the local directory, so the next
// open starts from the manifest.
func (idx *indexImpl) Close(ctx context.Context) {
	idx.closeOnce.Do(func() { idx.close(ctx) })
}

func (idx *indexImpl) close(ctx context.Context) {
	idx.lifetime.SetState(typeutil.LifetimeStateStopped)
	idx.cancel()
	idx.auth.Close()
	idx.lifetime.Wait()
	if err := idx.eng.Close(); err != nil {
		idx.logger.Warn(ctx, "failed to close primary key index engine", mlog.Err(err))
	}
	if idx.corrupted.Load() {
		if err := os.RemoveAll(idx.cfg.Dir); err != nil {
			idx.logger.Warn(ctx, "failed to remove the local primary key index after a corruption error", mlog.Err(err))
		} else {
			idx.logger.Info(ctx, "removed the local primary key index after a corruption error, the next open starts from the manifest")
		}
	}
	idx.logger.Info(ctx, "primary key index closed")
}

// markCorrupted records a corruption error that Apply or TruncateUntil saw.
// Close then removes the local directory.
func (idx *indexImpl) markCorrupted(err error) {
	if err != nil && errors.Is(err, pkroot.ErrCorrupted) {
		idx.corrupted.Store(true)
	}
}

// Ready implements Index.
func (idx *indexImpl) Ready() bool {
	select {
	case <-idx.ready:
		return true
	default:
		return false
	}
}

// markReady marks the Index ready, once: its committed table set is
// installed. It does nothing after the Index closed.
func (idx *indexImpl) markReady(ctx context.Context) {
	if !idx.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer idx.lifetime.Done()
	first := false
	idx.readyOnce.Do(func() {
		first = true
		close(idx.ready)
	})
	if !first {
		return
	}
	idx.cfg.OnRefresh(idx)
	idx.logger.Info(ctx, "primary key index ready")
}

// Handover runs one handover step under the lifetime of the Index: it
// prepares a handover when none is pending, else it commits the pending one.
// It reports whether a commit succeeded. It returns context.Canceled when
// the Index or taskCtx ends during the step, and the error of the step
// otherwise; the caller classifies it with manifest.IsFatal. The handover driver is
// the only caller, and it runs at most one step of an Index at a time.
func (idx *indexImpl) Handover(taskCtx context.Context) (committed bool, err error) {
	if !idx.lifetime.Add(typeutil.LifetimeStateWorking) {
		return false, context.Canceled
	}
	defer idx.lifetime.Done()
	ctx, cancel := context.WithCancel(idx.ctx)
	defer cancel()
	stop := context.AfterFunc(taskCtx, cancel)
	defer stop()

	committed, err = idx.handover(ctx)
	idx.cfg.OnRefresh(idx)
	if err != nil && ctx.Err() != nil {
		// the Index or the scheduler is closing, this is not a failure
		return false, context.Canceled
	}
	return committed, err
}

// handover prepares a handover when none is pending, and commits the pending
// one. It reports whether a commit succeeded.
func (idx *indexImpl) handover(ctx context.Context) (bool, error) {
	p := idx.pending.Load()
	if p == nil {
		// Clear the flag before the rotate. A write between the clear and
		// the rotate lands in the frozen generation and sets the flag again,
		// which only causes one extra handover.
		idx.dirtyFlag.Store(false)
		var err error
		p, err = idx.prepare(ctx)
		if err != nil || p == nil {
			// the writes may still be only in the active generation; the
			// OnRefresh after this step puts the Index back into the heap
			idx.markDirty()
			return false, err
		}
		// Record the pending before the upload. A failed upload then keeps
		// it, and the retry does not rotate again.
		idx.pending.Store(p)
	}
	err := idx.commit(ctx, p)
	if p.request == nil {
		// a failed DropFrozen must not be retried, the next engine open cleans up
		idx.pending.Store(nil)
		if err != nil {
			idx.logger.Warn(ctx, "failed to drop empty frozen generations", mlog.Err(err))
		}
		return true, nil
	}
	if err != nil {
		// keep the pending request: a retry must send the same request
		return false, err
	}
	idx.pending.Store(nil)
	idx.logger.Debug(ctx, "primary key index handover committed",
		mlog.Uint64("checkpointTimeTick", p.covered), mlog.Int("ssts", len(p.request.AddTables)))
	return true, nil
}
