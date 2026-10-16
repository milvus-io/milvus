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
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Index is the primary key index of one vchannel on a streaming node. It owns
// the engine, the write path authority on top of it, and the handover state
// of the vchannel. It reaches its pchannel only through the callbacks of its
// Config.
type Index struct {
	cfg          Config
	vchannel     string
	collectionID int64
	pkDataType   schemapb.DataType
	committedDir string
	eng          *engine.Engine
	auth         *authority.Authority
	logger       *mlog.Logger

	// ctx is canceled when the Index closes.
	ctx      context.Context
	cancel   context.CancelFunc
	lifetime *typeutil.Lifetime

	// ready is closed when the pchannel is claimed and, for an Index of an
	// existing collection, the manifest of the vchannel is looked up.
	ready     chan struct{}
	readyOnce sync.Once
	// sstBasePath is the object key prefix of the tables of the vchannel.
	sstBasePath string

	lastCommitted atomic.Uint64
	// dirtyFlag is set by every successful engine write and cleared right
	// before a handover rotates.
	dirtyFlag atomic.Bool
	// notFresh is set when the Index may miss writes that the WAL holds: the
	// local directories had files from before this open, or the vchannel has
	// a manifest. Such an Index serves writes, but it never hands over and it
	// stays out of the LastAcked heap. A handover would commit a checkpoint
	// that covers the missing writes.
	//
	// TODO: recovery replays the WAL into the Index and then clears this.
	notFresh atomic.Bool
	// pending is the prepared handover that is not committed yet.
	pending atomic.Pointer[pendingHandover]
	// installMu serializes the installs of committed table sets.
	installMu        sync.Mutex
	installedVersion int64
	installedPaths   map[sst.ID]string
}

// Params are the parameters of an Index. The Channel refreshes them.
type Params struct {
	// MemTableSize is the memtable size of the engine of one vchannel.
	MemTableSize uint64
	// LockStripes is the number of lock stripes of the authority of one vchannel.
	LockStripes int
	// ReuploadAfter is the age of an upload after which a retried commit
	// uploads the tables again.
	ReuploadAfter time.Duration
}

// Config is what an Index needs from its Channel. The callbacks are how the
// Index reaches the pchannel: it does not know the Channel.
type Config struct {
	VChannel     string
	CollectionID int64
	PKDataType   schemapb.DataType
	// Term is the term of the pchannel, sent with every flush.
	Term int64
	// Ctx ends when the Channel closes. The Index derives its own from it.
	Ctx context.Context
	// Logger carries the fields of the pchannel.
	Logger *mlog.Logger

	// Root is the local root directory of the indexes of the node.
	Root string
	// StorageRootPath is the root path of the object storage.
	StorageRootPath string
	Shared          *engine.SharedResources
	AllocID         func(ctx context.Context) (int64, error)
	Store           TableStore
	Committer       Committer
	Params          func() Params

	// OnWriterBegin starts a writer in the watermark of the pchannel, see
	// authority.Config.OnWriterBegin.
	OnWriterBegin func() (end func())
	// LastApplied returns the lastAppliedTimeTick of the pchannel: the time
	// tick that a handover covers.
	LastApplied func() uint64
	// OnDirtied runs when the Index gets its first write since its last
	// rotate. OnRefresh runs when the dirty state or LastCommittedTimeTick
	// of the Index may have changed: after a handover step and after
	// MarkReady. The Channel keeps its LastAcked heap with them.
	OnDirtied func(*Index)
	OnRefresh func(*Index)
}

// Open opens the engine of a vchannel and builds the Index on top of it. The
// Channel marks it ready after the claim.
//
// The local directories of the vchannel may hold files from before this
// open: a crash, or a Remove that kept them. Such an Index may miss writes
// that the WAL holds, so it is not fresh, see Fresh. Its files are removed
// and the Index starts empty. The engine dir and the committed dir are
// checked together, because a table in committed/ is only useful with the
// engine state that installed it, and both are rebuilt by the writes
// since. A file in only one of the two directories, such as the temporary
// file of a download that did not finish or the rest of a wipe that did not
// finish, counts too.
func Open(ctx context.Context, cfg Config) (*Index, error) {
	params := cfg.Params()
	dir := Dir(cfg.Root, cfg.VChannel)
	// engine.Destroy removes the engine dir, so the committed tables live outside it
	engineDir := filepath.Join(dir, "engine")
	committedDir := filepath.Join(dir, "committed")
	leftover, err := hasLeftover(engineDir, committedDir)
	if err != nil {
		return nil, err
	}
	if leftover {
		if err := wipeLocal(engineDir, committedDir); err != nil {
			return nil, err
		}
		cfg.Logger.Info(ctx, "removed the local primary key index files from before this open, the index starts empty and does not hand over",
			mlog.FieldVChannel(cfg.VChannel))
	}
	eng, err := engine.Open(ctx, engine.Config{
		Dir:          engineDir,
		VChannel:     cfg.VChannel,
		Shared:       cfg.Shared,
		AllocID:      cfg.AllocID,
		MemTableSize: params.MemTableSize,
	})
	if err != nil {
		return nil, err
	}
	idxCtx, cancel := context.WithCancel(cfg.Ctx)
	idx := &Index{
		cfg:          cfg,
		vchannel:     cfg.VChannel,
		collectionID: cfg.CollectionID,
		pkDataType:   cfg.PKDataType,
		committedDir: committedDir,
		eng:          eng,
		logger:       cfg.Logger.With(mlog.FieldVChannel(cfg.VChannel), mlog.FieldCollectionID(cfg.CollectionID)),
		ctx:          idxCtx,
		cancel:       cancel,
		lifetime:     typeutil.NewLifetime(),
		ready:        make(chan struct{}),
		sstBasePath:  pkroot.SSTBasePath(cfg.StorageRootPath, cfg.CollectionID, cfg.VChannel),
	}
	idx.notFresh.Store(leftover)
	idx.auth = authority.New(dirtyRW{eng: eng, idx: idx}, authority.Config{
		LockStripes:   params.LockStripes,
		OnWriterBegin: cfg.OnWriterBegin,
	})
	return idx, nil
}

// VChannel returns the vchannel of the Index.
func (idx *Index) VChannel() string { return idx.vchannel }

// CollectionID returns the collection of the Index.
func (idx *Index) CollectionID() int64 { return idx.collectionID }

// Logger returns the logger of the Index, with its vchannel and collection.
func (idx *Index) Logger() *mlog.Logger { return idx.logger }

// HasPending reports whether a prepared handover waits for its commit.
func (idx *Index) HasPending() bool { return idx.pending.Load() != nil }

// ActiveBytes returns the bytes that the active generation of the engine
// holds, the size that triggers a handover.
func (idx *Index) ActiveBytes() uint64 { return idx.eng.Stats().ActiveBytes }

// Dir is the local directory of the Index of a vchannel under root. It holds
// the engine directory and the committed directory.
func Dir(root, vchannel string) string {
	return filepath.Join(root, vchannel)
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

// fresh reports whether the Index can hand over: the node has a Committer,
// and notFresh is not set.
func (idx *Index) Fresh() bool {
	return idx.cfg.Committer != nil && !idx.notFresh.Load()
}

// dirtyRW is the engine as the write path sees it. A successful write marks
// the Index dirty, so that a later handover publishes it.
type dirtyRW struct {
	eng engine.RW
	idx *Index
}

func (w dirtyRW) MultiGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return w.eng.MultiGet(ctx, keys)
}

func (w dirtyRW) Write(ctx context.Context, muts []engine.Mutation) error {
	if err := w.eng.Write(ctx, muts); err != nil {
		return err
	}
	w.idx.markDirty()
	return nil
}

// markDirty sets the dirty flag. On the change from clean to dirty it adds
// the Index to the LastAcked heap of its Channel, unless the Index is not
// fresh.
func (idx *Index) markDirty() {
	if idx.dirtyFlag.CompareAndSwap(false, true) && idx.Fresh() {
		idx.cfg.OnDirtied(idx)
	}
}

// dirty reports whether the Index has writes that are not committed: writes
// since the last rotate, frozen generations, or a pending handover.
func (idx *Index) Dirty() bool {
	return idx.dirtyFlag.Load() || idx.pending.Load() != nil || len(idx.eng.FrozenGenerations()) > 0
}

// Authority returns the write path authority of the vchannel. The WAL
// interceptor decides every write through it. Each open WriteGuard holds
// the watermark of the pchannel back and keeps the Index open.
func (idx *Index) Authority() *authority.Authority {
	return idx.auth
}

// close stops the Index. It waits for a running handover and
// every open WriteGuard, then closes the engine. It keeps the local files.
func (idx *Index) Close(ctx context.Context) {
	idx.lifetime.SetState(typeutil.LifetimeStateStopped)
	idx.cancel()
	idx.auth.Close()
	idx.lifetime.Wait()
	if err := idx.eng.Close(); err != nil {
		idx.logger.Warn(ctx, "failed to close primary key index engine", mlog.Err(err))
		return
	}
	idx.logger.Info(ctx, "primary key index closed")
}

// currentInstalledVersion returns the version of the installed manifest, 0
// when no manifest is installed.
func (idx *Index) currentInstalledVersion() int64 {
	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	return idx.installedVersion
}

// lastCommittedTimeTick returns the checkpoint of the last successful
// handover: all pkindex changes of messages with a time tick <= the returned
// value are committed.
func (idx *Index) LastCommittedTimeTick() uint64 { return idx.lastCommitted.Load() }

// installIfNewer installs the tables of m when m is newer than the installed
// manifest, and retires gens in the same step. local maps the tables of this
// handover to their local files.
//
// It finds the local file of every table of m in this order: local, the
// tables that the Index installed before, the committed directory (the links
// survive a restart), and last a download from the store. It downloads
// outside installMu and checks the version again under it.
func (idx *Index) installIfNewer(ctx context.Context, m ManifestView, local map[sst.ID]string, gens ...engine.Generation) error {
	idx.installMu.Lock()
	newer := m.Version > idx.installedVersion
	known := make(map[sst.ID]string, len(m.Tables))
	for _, ref := range m.Tables {
		if p, ok := idx.installedPaths[ref.ID]; ok {
			known[ref.ID] = p
		}
	}
	idx.installMu.Unlock()

	if !newer {
		return idx.retireCovered(ctx, m, gens)
	}

	tables := make([]engine.CommittedTable, 0, len(m.Tables))
	for _, ref := range m.Tables {
		path, err := idx.resolveLocal(ctx, m.SSTBasePath, ref.ID, local, known)
		if err != nil {
			return err
		}
		tables = append(tables, engine.CommittedTable{Info: ref, Path: path})
	}

	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	if m.Version <= idx.installedVersion {
		return idx.retireCoveredLocked(ctx, m, gens)
	}
	if err := idx.eng.InstallCommitted(ctx, tables, gens...); err != nil {
		return err
	}
	// TODO: no code deletes a local committed file. A compaction must delete
	// the files of the tables that leave the set.
	paths := make(map[sst.ID]string, len(tables))
	for _, t := range tables {
		paths[t.Info.ID] = t.Path
	}
	idx.installedVersion = m.Version
	idx.installedPaths = paths
	return nil
}

// retireCovered drops gens when a newer manifest than m is installed.
func (idx *Index) retireCovered(ctx context.Context, m ManifestView, gens []engine.Generation) error {
	if len(gens) == 0 {
		return nil
	}
	idx.installMu.Lock()
	defer idx.installMu.Unlock()
	return idx.retireCoveredLocked(ctx, m, gens)
}

// retireCoveredLocked drops gens when the installed manifest is not older
// than m. m covers gens, and a later version keeps covering what m covers,
// so the installed tables hold the data of gens. Without the drop, gens stay
// frozen, and the next handover sends their committed tables again. The
// caller holds installMu.
func (idx *Index) retireCoveredLocked(ctx context.Context, m ManifestView, gens []engine.Generation) error {
	for _, g := range gens {
		if err := idx.eng.DropFrozen(ctx, g); err != nil {
			idx.logger.Warn(ctx, "failed to drop a frozen generation that a newer manifest covers",
				mlog.Int64("generation", int64(g)), mlog.Int64("manifestVersion", m.Version), mlog.Err(err))
		}
	}
	return nil
}

// resolveLocal returns the local file of table id. It downloads the table
// when no local file exists.
func (idx *Index) resolveLocal(ctx context.Context, basePath string, id sst.ID, local, known map[sst.ID]string) (string, error) {
	if p, ok := local[id]; ok {
		return p, nil
	}
	if p, ok := known[id]; ok {
		return p, nil
	}
	path := committedPath(idx.committedDir, id)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	} else if !os.IsNotExist(err) {
		return "", pkroot.MarkIO(err, "stat committed sst %s", path)
	}
	if err := os.MkdirAll(idx.committedDir, 0o755); err != nil {
		return "", pkroot.MarkIO(err, "create committed dir %s", idx.committedDir)
	}
	if err := idx.cfg.Store.DownloadFile(ctx, pkroot.SSTKey(basePath, int64(id)), path); err != nil {
		return "", err
	}
	idx.logger.Info(ctx, "downloaded a committed primary key index table", mlog.Int64("sstID", int64(id)))
	return path, nil
}

// readyDone reports whether the Index is ready: the pchannel is claimed and
// the manifest lookup of the vchannel is done.
func (idx *Index) Ready() bool {
	select {
	case <-idx.ready:
		return true
	default:
		return false
	}
}

// markReady marks the Index ready, once. found reports whether the vchannel
// has a manifest. A manifest of any version makes the Index not fresh: an
// owner committed it, and this Index can miss the writes of that owner that
// it neither handed over nor replayed. It does nothing after the Index closed.
//
// The engine holds no frozen generation here: openIndex removed the data from
// before the open, and no handover starts before the Index is ready. So no
// generation needs to retire through the manifest.
//
// TODO: recovery opens the Index with the data from before the open. It reads
// the manifest body from the BodyKey of the lookup, and it must retire the
// frozen generations whose staged tables the manifest already holds, else the
// first handover sends them again and the manifest service rejects them as
// duplicates. After a compaction, a match by staged table IDs can add tables
// back that the compaction removed. Reading the staged list must not stage a
// generation, so the engine needs an accessor that only reads the STAGED list.
func (idx *Index) MarkReady(ctx context.Context, found bool) {
	if !idx.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer idx.lifetime.Done()
	first := false
	idx.readyOnce.Do(func() {
		first = true
		if found {
			// The engine holds only writes from after this open. Keep them,
			// but never hand them over: the manifest can cover writes that
			// this Index does not have.
			idx.notFresh.Store(true)
		}
		close(idx.ready)
	})
	if !first {
		return
	}
	idx.cfg.OnRefresh(idx)
	idx.logger.Info(ctx, "primary key index ready", mlog.Bool("manifestFound", found), mlog.Bool("fresh", idx.Fresh()))
}

// Handover runs one handover step under the lifetime of the Index: it
// prepares a handover when none is pending, else it commits the pending one.
// It reports whether a commit succeeded. It returns context.Canceled when
// the Index or taskCtx ends during the step, and the error of the step
// otherwise; the caller classifies it with IsFatal. The handover driver is
// the only caller, and it runs at most one step of an Index at a time.
func (idx *Index) Handover(taskCtx context.Context) (committed bool, err error) {
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
func (idx *Index) handover(ctx context.Context) (bool, error) {
	p := idx.pending.Load()
	if p == nil {
		// Clear the flag before the rotate. A write between the clear and
		// the rotate lands in the frozen generation and sets the flag again,
		// which only causes one extra handover.
		idx.dirtyFlag.Store(false)
		var err error
		p, err = idx.prepare(ctx)
		if err != nil || p == nil {
			// the writes may still be only in the active generation
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
