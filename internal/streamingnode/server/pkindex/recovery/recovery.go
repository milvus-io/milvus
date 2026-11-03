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
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/indexmanager"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RecoverConfig is what the WAL recovery gives the recovery of the primary
// key index.
type RecoverConfig struct {
	// PChannelConfig goes to Manager.New as it is.
	indexmanager.PChannelConfig
	// Metas are the vchannels of the pchannel from the catalog, by vchannel.
	Metas map[string]*streamingpb.VChannelMeta
	// Tail returns the WAL tail as the recovery storage sees it now. The
	// observe calls it at every persisted TimeTick, under the lock of the
	// storage, and passes the snapshot to the index, which evaluates its
	// handover triggers with it. Nil means an unknown tail, which never
	// starts a handover by size.
	Tail func() indexmanager.WALTailSnapshot
}

// target is one indexed vchannel during the recovery.
type target struct {
	idx        *index.Index
	pkFieldID  int64
	pkDataType schemapb.DataType
}

// pkField is the primary key field of a target: the field of the collection
// schema the index of its vchannel indexes.
type pkField struct {
	fieldID  int64
	dataType schemapb.DataType
}

// Recovery is the primary key index module of the WAL recovery of one
// pchannel. The recovery feeds it every persisted WAL message with
// ObserveMessage, from the global checkpoint to the recovery barrier and
// then the live messages through the same call; it calls Run once at the
// barrier, and Close in every case. One goroutine calls Run and Close, Close
// last. Close is idempotent; Run after Close fails.
type Recovery struct {
	mgr     *indexmanager.Manager
	m       *indexmanager.PChannelIndexManager
	tail    func() indexmanager.WALTailSnapshot
	onFatal func(error)
	logger  *mlog.Logger

	mu       sync.Mutex
	targets  map[string]*target // by vchannel
	// pkFields are the primary key fields of the targets, by vchannel.
	pkFields map[string]pkField
	err      error // the first error of the replay before Run
	finished bool
	// stopped is set by an observe error after Run: the WAL is closing, or
	// the error went to OnFatal. The observe applies nothing after it and
	// does not move the observed time tick, see fail.
	stopped bool
	closed  bool
	// wipe holds the vchannels whose local data is not trusted after a
	// corruption error. Close removes their local directories, so the next
	// open starts from the manifest.
	wipe map[string]string // the local directory by vchannel
}

// errClosed reports a call on a closed Recovery or a stopped Manager. It
// wraps engine.ErrClosed and is marked pkroot.ErrUnavailable, like the
// closed error of the root package.
func errClosed(what string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index recovery of %s is closed", what), pkroot.ErrUnavailable)
}

// Recover creates the primary key index of the pchannel, opens the index of
// every vchannel in metas whose meta has an enabled primary key index, and
// reads the manifests. The handover
// driver does not run until Run. An error leaves
// nothing open and makes the WAL open fail.
func Recover(ctx context.Context, mgr *indexmanager.Manager, cfg RecoverConfig) (*Recovery, error) {
	m := mgr.New(cfg.PChannelConfig)
	if m.Closed() {
		// a stopped Manager returns a closed PChannelIndexManager
		return nil, errClosed("pchannel " + cfg.PChannel.Name)
	}
	r := &Recovery{
		mgr:      mgr,
		m:        m,
		tail:     cfg.Tail,
		onFatal:  cfg.OnFatal,
		logger:   mlog.With(mlog.FieldModule("pkindex-recovery"), mlog.FieldPChannel(cfg.PChannel.Name), mlog.Int64("term", cfg.PChannel.Term)),
		targets:  make(map[string]*target),
		pkFields: make(map[string]pkField),
		wipe:     make(map[string]string),
	}
	if r.tail == nil {
		r.tail = func() indexmanager.WALTailSnapshot { return indexmanager.WALTailSnapshot{} }
	}
	if r.onFatal == nil {
		r.onFatal = func(err error) {
			r.logger.Error(context.TODO(), "primary key index recovery failed, no handler marks the WAL unavailable", mlog.Err(err))
		}
	}
	var specs []indexmanager.IndexSpec
	for vchannel, meta := range cfg.Metas {
		info := meta.GetCollectionInfo()
		if meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL || info.GetPkIndex() == nil {
			continue
		}
		var schema *schemapb.CollectionSchema
		if schemas := info.GetSchemas(); len(schemas) > 0 {
			schema = schemas[len(schemas)-1].GetSchema()
		}
		f, err := pkFieldOf(vchannel, info.GetCollectionId(), info.GetPkIndex(), schema)
		if err != nil {
			r.Close(ctx)
			return nil, err
		}
		specs = append(specs, indexmanager.IndexSpec{
			VChannel:           vchannel,
			CollectionID:       info.GetCollectionId(),
			IndexID:            info.GetPkIndex().GetIndexId(),
			CheckpointTimeTick: info.GetPkIndex().GetCheckpointTimeTick(),
		})
		r.pkFields[vchannel] = f
	}
	indexes, err := m.RecoverIndexes(ctx, specs)
	if err != nil {
		r.Close(ctx)
		return nil, err
	}
	for _, idx := range indexes {
		f := r.pkFields[idx.VChannel()]
		r.targets[idx.VChannel()] = &target{idx: idx, pkFieldID: f.fieldID, pkDataType: f.dataType}
	}
	return r, nil
}

// pkFieldOf returns the primary key field that pkIndex names in schema, the
// latest schema of the collection, or an error when the field is missing, is
// not the primary key, or has an unsupported data type.
func pkFieldOf(vchannel string, collectionID int64, pkIndex *streamingpb.PKIndexOfVChannel, schema *schemapb.CollectionSchema) (pkField, error) {
	field := typeutil.GetField(schema, pkIndex.GetFieldId())
	if field == nil || !field.GetIsPrimaryKey() {
		return pkField{}, merr.WrapErrServiceInternalMsg("primary key index %d of vchannel %s names field %d, which is not the primary key", pkIndex.GetIndexId(), vchannel, pkIndex.GetFieldId())
	}
	dt := field.GetDataType()
	if dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
		return pkField{}, merr.WrapErrServiceInternalMsg("primary key of collection %d has the unsupported data type %s", collectionID, dt)
	}
	return pkField{fieldID: field.GetFieldID(), dataType: dt}, nil
}

// openIndex opens the index pkIndex of a vchannel and makes the vchannel a
// target. The primary key field is the field pkIndex names in schema, the
// latest schema of the collection. A field that is missing or of an
// unsupported data type is an error.
func (r *Recovery) openIndex(ctx context.Context, vchannel string, collectionID int64, pkIndex *streamingpb.PKIndexOfVChannel, schema *schemapb.CollectionSchema) error {
	f, err := pkFieldOf(vchannel, collectionID, pkIndex, schema)
	if err != nil {
		return err
	}
	idx, _, err := r.m.OpenIndex(ctx, indexmanager.IndexSpec{
		VChannel:           vchannel,
		CollectionID:       collectionID,
		IndexID:            pkIndex.GetIndexId(),
		CheckpointTimeTick: pkIndex.GetCheckpointTimeTick(),
	})
	if err != nil {
		return errors.Wrapf(err, "open the primary key index of vchannel %s", vchannel)
	}
	r.targets[vchannel] = &target{idx: idx, pkFieldID: f.fieldID, pkDataType: f.dataType}
	return nil
}

// Run marks the recovery barrier. It starts the handover driver, whose Start
// submits the install of every open index with a manifest; ObserveMessage keeps
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
	r.m.Start()
	r.logger.Info(ctx, "primary key index recovery done", mlog.Int("indexes", len(r.targets)))
	return nil
}

// Close releases everything Recover opened: it closes the index of the
// pchannel, which cancels the install tasks and waits for them. It then
// removes the local directory of every vchannel that a corruption error
// marked, see wipe. A second Close does nothing.
func (r *Recovery) Close(ctx context.Context) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	wipe := r.wipe
	r.wipe = nil
	r.mu.Unlock()
	r.m.Close()
	for vchannel, dir := range wipe {
		if err := os.RemoveAll(dir); err != nil {
			r.logger.Warn(ctx, "failed to remove the local primary key index after a corruption error", mlog.FieldVChannel(vchannel), mlog.String("dir", dir), mlog.Err(err))
			continue
		}
		r.logger.Info(ctx, "removed the local primary key index after a corruption error, the next open starts from the manifest", mlog.FieldVChannel(vchannel))
	}
}

// LastAcked returns the LastAcked of the index of the pchannel, see
// PChannelIndexManager.LastAcked.
func (r *Recovery) LastAcked() uint64 { return r.m.LastAcked() }

// Manager returns the index of the pchannel. The WAL recovery puts it in the
// recovery snapshot for the interceptor, which uses it and does not own it.
func (r *Recovery) Manager() *indexmanager.PChannelIndexManager { return r.m }

// retryOptions returns the retry options of the recovery: no attempt limit,
// a backoff from BackoffBase up to MaxBackoff.
func (r *Recovery) retryOptions() []retry.Option {
	p := r.mgr.Params()
	return []retry.Option{retry.AttemptAlways(), retry.Sleep(p.BackoffBase), retry.MaxSleepTime(p.MaxBackoff)}
}
