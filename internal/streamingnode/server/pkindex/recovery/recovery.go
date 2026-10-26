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

// Package recovery recovers the primary key index of a pchannel when its WAL
// opens: it opens the indexes of the existing collections, reads the
// manifests, replays the WAL messages the recovery framework delivers, and
// installs the committed table sets. The WAL
// recovery storage owns a Recovery from Recover to Close.
package recovery

import (
	"context"
	"os"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Param is what the WAL recovery gives the recovery of the primary key index.
type Param struct {
	// PChannelConfig goes to Manager.New as it is.
	pkindex.PChannelConfig
	// Metas are the vchannels of the pchannel from the catalog, by vchannel.
	Metas map[string]*streamingpb.VChannelMeta
	// Sched runs the install tasks. Nil means the scheduler of the Manager.
	Sched nodescheduler.Scheduler
	// ManifestReadConcurrency bounds the parallel manifest body reads. A
	// value at or below 0 means 64.
	ManifestReadConcurrency int
	// Tail returns the WAL tail as the recovery storage sees it now. The
	// observe calls it at every persisted TimeTick, under the lock of the
	// storage, and passes the snapshot to the index, which evaluates its
	// handover triggers with it. Nil means an unknown tail, which never
	// starts a handover by size.
	Tail func() pkindex.WALTailSnapshot
}

// target is one indexed vchannel during the recovery.
type target struct {
	idx        *index.Index
	pkFieldID  int64
	pkDataType schemapb.DataType
}

// Recovery is the primary key index module of the WAL recovery of one
// pchannel. The recovery feeds it every persisted WAL message with
// ObserveMessage, from the global checkpoint to the recovery barrier and
// then the live messages through the same call; it calls Run once at the
// barrier, and Close in every case. One goroutine calls Run and Close, Close
// last. Close is idempotent; Run after Close fails.
type Recovery struct {
	mgr     *pkindex.Manager
	m       *pkindex.PChannelIndexManager
	sched   nodescheduler.Scheduler
	tail    func() pkindex.WALTailSnapshot
	onFatal func(error)
	logger  *mlog.Logger

	mu       sync.Mutex
	targets  map[string]*target // by vchannel
	pending  []*index.Index     // the indexes with a manifest, installed at Run
	err      error              // the first error of the replay before Run
	finished bool
	// stopped is set by an observe error after Run: the WAL is closing, or
	// the error went to OnFatal. The observe applies nothing after it and
	// does not move the observed time tick, see fail.
	stopped bool
	closed  bool
	// wipe holds the vchannels whose local data is not trusted after a
	// corruption error. Close removes their local directories, so the next
	// open starts from the manifest.
	wipe    map[string]struct{}
	handles []nodescheduler.TaskHandle
}

// errClosed reports a call on a closed Recovery or a stopped Manager. It
// wraps engine.ErrClosed and is marked pkroot.ErrUnavailable, like the
// closed error of the root package.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index recovery of %s is closed", what), pkroot.ErrUnavailable)
}

// Recover creates the primary key index of the pchannel, opens the index of
// every indexed collection in metas and reads the manifests. The handover
// driver does not run until Run. An error leaves
// nothing open and makes the WAL open fail.
func Recover(ctx context.Context, mgr *pkindex.Manager, p Param) (*Recovery, error) {
	m := mgr.New(p.PChannelConfig)
	if m.Closed() {
		// a stopped Manager returns a closed PChannelIndexManager
		return nil, errClosed("pchannel " + p.PChannel.Name)
	}
	r := &Recovery{
		mgr:     mgr,
		m:       m,
		sched:   p.Sched,
		tail:    p.Tail,
		onFatal: p.OnFatal,
		logger:  mlog.With(mlog.FieldModule("pkindex-recovery"), mlog.FieldPChannel(p.PChannel.Name), mlog.Int64("term", p.PChannel.Term)),
		targets: make(map[string]*target),
		wipe:    make(map[string]struct{}),
	}
	if r.sched == nil {
		r.sched = mgr.Scheduler()
	}
	if r.tail == nil {
		r.tail = func() pkindex.WALTailSnapshot { return pkindex.WALTailSnapshot{} }
	}
	if r.onFatal == nil {
		r.onFatal = func(err error) {
			r.logger.Error(context.TODO(), "primary key index recovery failed, no handler marks the WAL unavailable", mlog.Err(err))
		}
	}
	for vchannel, meta := range p.Metas {
		if meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL || meta.GetCollectionInfo() == nil {
			continue
		}
		schemas := meta.GetCollectionInfo().GetSchemas()
		if len(schemas) == 0 {
			continue
		}
		// The last schema of the vchannel names the primary key field.
		if err := r.addCollection(ctx, vchannel, meta.GetCollectionInfo().GetCollectionId(), schemas[len(schemas)-1].GetSchema()); err != nil {
			r.Close(ctx)
			return nil, err
		}
	}
	if err := r.lookupManifests(ctx, mgr.Committer(), p.ManifestReadConcurrency); err != nil {
		r.Close(ctx)
		return nil, err
	}
	return r, nil
}

// addCollection opens the index of a collection, when its primary key gets
// one. An unsupported primary key type is an error.
func (r *Recovery) addCollection(ctx context.Context, vchannel string, collectionID int64, schema *schemapb.CollectionSchema) error {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return err
	}
	if pkField.GetAutoID() {
		return nil
	}
	dt := pkField.GetDataType()
	if dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
		return merr.WrapErrServiceInternalMsg("primary key of collection %d has the unsupported data type %s", collectionID, dt)
	}
	idx, _, err := r.m.CreateCollection(ctx, vchannel, collectionID, dt)
	if err != nil {
		return errors.Wrapf(err, "open the primary key index of vchannel %s", vchannel)
	}
	r.targets[vchannel] = &target{idx: idx, pkFieldID: pkField.GetFieldID(), pkDataType: dt}
	return nil
}

// Run marks the recovery barrier. It submits the install of every open
// index with a manifest and starts the handover driver; ObserveMessage keeps
// applying the messages after it. It returns the first error of the replay
// before it, which makes the WAL open fail, and an error marked
// pkroot.ErrUnavailable after Close. A second Run does nothing.
func (r *Recovery) Run(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errClosed("pchannel " + r.m.Info().Name)
	}
	if r.err != nil {
		return r.err
	}
	if r.finished {
		return nil
	}
	r.finished = true
	installs := 0
	for _, idx := range r.pending {
		if t, ok := r.targets[idx.VChannel()]; !ok || t.idx != idx {
			// the replay dropped the collection
			continue
		}
		installs++
		r.handles = append(r.handles, r.sched.Submit(&installTask{idx: idx, params: r.mgr.Params, onFatal: r.onFatal, logger: r.logger}))
	}
	r.m.Start()
	r.logger.Info(ctx, "primary key index recovery done", mlog.Int("indexes", len(r.targets)), mlog.Int("installs", installs))
	return nil
}

// Close releases everything Recover opened: it cancels the install tasks,
// waits for them, and closes the index of the pchannel. It then removes the
// local directory of every vchannel that a corruption error marked, see
// wipe. A second Close does nothing.
func (r *Recovery) Close(ctx context.Context) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	handles := r.handles
	r.handles = nil
	wipe := r.wipe
	r.wipe = nil
	r.mu.Unlock()
	for _, h := range handles {
		h.Cancel()
	}
	for _, h := range handles {
		_ = h.Wait(ctx)
	}
	r.m.Close()
	for vchannel := range wipe {
		dir := index.Dir(r.mgr.Root(), vchannel)
		if err := os.RemoveAll(dir); err != nil {
			r.logger.Warn(ctx, "failed to remove the local primary key index after a corruption error", mlog.FieldVChannel(vchannel), mlog.String("dir", dir), mlog.Err(err))
			continue
		}
		r.logger.Info(ctx, "removed the local primary key index after a corruption error, the next open starts from the manifest", mlog.FieldVChannel(vchannel))
	}
}

// The classes of an observe error, see fail and the pkindex_fatal_total metric.
const (
	errorClassCorrupted   = "corrupted"
	errorClassIO          = "io"
	errorClassClosed      = "closed"
	errorClassCanceled    = "canceled"
	errorClassUnavailable = "unavailable"
	errorClassOther       = "other"
)

// errorClass classifies an observe error by the markers of the pkindex
// packages, the most specific first.
func errorClass(err error) string {
	switch {
	case errors.Is(err, pkroot.ErrCorrupted):
		return errorClassCorrupted
	case errors.Is(err, engine.ErrClosed):
		return errorClassClosed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return errorClassCanceled
	case errors.Is(err, pkroot.ErrIO):
		return errorClassIO
	case errors.Is(err, pkroot.ErrUnavailable):
		return errorClassUnavailable
	default:
		return errorClassOther
	}
}

// LastAcked returns the LastAcked of the index of the pchannel, see
// PChannelIndexManager.LastAcked.
func (r *Recovery) LastAcked() uint64 { return r.m.LastAcked() }

// Manager returns the index of the pchannel. The WAL recovery puts it in the
// recovery snapshot for the interceptor, which uses it and does not own it.
func (r *Recovery) Manager() *pkindex.PChannelIndexManager { return r.m }
