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
// opens: it opens the indexes of the existing collections, claims the
// pchannel, reads the manifests, replays the WAL messages the recovery
// framework delivers, and installs the committed table sets. The WAL
// recovery storage owns a Recovery from Recover to Close.
package recovery

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Param is what the WAL recovery gives the recovery of the primary key index.
type Param struct {
	Info types.PChannelInfo
	// Metas are the vchannels of the pchannel from the catalog, by vchannel.
	Metas map[string]*streamingpb.VChannelMeta
	// Seed is the time tick of the WAL checkpoint the replay starts from.
	Seed uint64
	// Tail returns the WAL recovery tail. Nil is allowed.
	Tail func() pkindex.WALTail
	// OnFatal marks the WAL unavailable. It must not block. Nil only logs.
	OnFatal func(error)
	// Sched runs the install tasks. Nil means the scheduler of the Manager.
	Sched nodescheduler.Scheduler
	// ManifestReadConcurrency bounds the parallel manifest body reads. A
	// value at or below 0 means 64.
	ManifestReadConcurrency int
}

// target is one indexed vchannel during the recovery.
type target struct {
	idx        *index.Index
	pkFieldID  int64
	pkDataType schemapb.DataType
}

// Recovery is the recovery of one pchannel. The caller feeds it the WAL
// messages up to the recovery barrier with Observe, ends it with Finish, and
// releases it with Close in every case. One goroutine calls Finish and
// Close, Close last. Close is idempotent; Finish after Close fails.
type Recovery struct {
	mgr     *pkindex.Manager
	m       *pkindex.PChannelIndexManager
	sched   nodescheduler.Scheduler
	onFatal func(error)
	logger  *mlog.Logger

	mu       sync.Mutex
	targets  map[string]*target // by vchannel
	pending  []*index.Index     // the indexes with a manifest, installed at Finish
	err      error              // the first error of the replay
	finished bool
	closed   bool
	handles  []nodescheduler.TaskHandle
}

// errClosed reports a call on a closed Recovery or a stopped Manager. It
// wraps engine.ErrClosed and is marked pkroot.ErrUnavailable, like the
// closed error of the root package.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index recovery of %s is closed", what), pkroot.ErrUnavailable)
}

// Recover creates the primary key index of the pchannel, opens the index of
// every indexed collection in metas, claims the pchannel and reads the
// manifests. The handover driver does not run until Finish. An error leaves
// nothing open and makes the WAL open fail.
func Recover(ctx context.Context, mgr *pkindex.Manager, p Param) (*Recovery, error) {
	m := mgr.New(pkindex.PChannelConfig{PChannel: p.Info, Seed: p.Seed, Tail: p.Tail, OnFatal: p.OnFatal})
	if mgr.Get(p.Info.Name) != m {
		// a stopped Manager returns a closed PChannelIndexManager
		return nil, errClosed("pchannel " + p.Info.Name)
	}
	r := &Recovery{
		mgr:     mgr,
		m:       m,
		sched:   p.Sched,
		onFatal: p.OnFatal,
		logger:  mlog.With(mlog.FieldModule("pkindex-recovery"), mlog.FieldPChannel(p.Info.Name), mlog.Int64("term", p.Info.Term)),
		targets: make(map[string]*target),
	}
	if r.sched == nil {
		r.sched = mgr.Scheduler()
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
	if err := r.claim(ctx, mgr.Committer(), p.ManifestReadConcurrency); err != nil {
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

// Finish ends the replay at the recovery barrier. It submits the install of
// every open index with a manifest and starts the handover driver. It
// returns the first error of the replay, which makes the WAL open fail, and
// an error marked pkroot.ErrUnavailable after Close. A second Finish does
// nothing.
func (r *Recovery) Finish(ctx context.Context) error {
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
// waits for them, and closes the index of the pchannel. After it,
// Manager.Get does not find the pchannel. A second Close does nothing.
func (r *Recovery) Close(ctx context.Context) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	handles := r.handles
	r.handles = nil
	r.mu.Unlock()
	for _, h := range handles {
		h.Cancel()
	}
	for _, h := range handles {
		_ = h.Wait(ctx)
	}
	r.m.Close()
}

// LastAcked returns the LastAcked of the index of the pchannel, see
// PChannelIndexManager.LastAcked.
func (r *Recovery) LastAcked() uint64 { return r.m.LastAcked() }
