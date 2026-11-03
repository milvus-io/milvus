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
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/indexmanager"
	vchannel "github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Recovery consumes ordered WAL messages for one pchannel and owns its indexes.
type Recovery interface {
	ObserveMessage(ctx context.Context, msg message.ImmutableMessage) func(context.Context)
	Run(ctx context.Context) error
	Close(ctx context.Context)
	LastAcked() uint64
	PChannel() indexmanager.PChannel
}

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
	// PKIndexState reads the state after the vchannel module observes a message.
	PKIndexState func(string) (vchannel.PKIndexState, bool)
}

// target is one indexed vchannel during the recovery.
type target struct {
	idx                index.Index
	pkFieldID          int64
	pkDataType         schemapb.DataType
	checkpointTimeTick uint64
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
type recovery struct {
	m            indexmanager.PChannel
	info         types.PChannelInfo
	pkIndexState func(string) (vchannel.PKIndexState, bool)
	tail         func() indexmanager.WALTailSnapshot
	onFatal      func(error)
	logger       *mlog.Logger

	mu      sync.Mutex
	targets map[string]*target // by vchannel
	// pkFields are the primary key fields of the targets, by vchannel.
	pkFields map[string]pkField
	err      error // the first error of the replay before Run
	finished bool
	// stopped is set by an observe error after Run: the WAL is closing, or
	// the error went to OnFatal. The observe applies nothing after it and
	// does not move the observed time tick, see fail.
	stopped bool
	closed  bool
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
func Recover(ctx context.Context, mgr indexmanager.Manager, cfg RecoverConfig) (Recovery, error) {
	logger := mlog.With(mlog.FieldModule("pkindex-recovery"), mlog.FieldPChannel(cfg.PChannel.Name), mlog.Int64("term", cfg.PChannel.Term))
	if cfg.OnFatal == nil {
		cfg.OnFatal = func(err error) {
			logger.Error(context.TODO(), "primary key index recovery failed, no handler marks the WAL unavailable", mlog.Err(err))
		}
	}
	m := mgr.New(cfg.PChannelConfig)
	r := &recovery{
		m:            m,
		info:         cfg.PChannel,
		pkIndexState: cfg.PKIndexState,
		tail:         cfg.Tail,
		onFatal:      cfg.OnFatal,
		logger:       logger,
		targets:      make(map[string]*target),
		pkFields:     make(map[string]pkField),
	}
	if r.tail == nil {
		r.tail = func() indexmanager.WALTailSnapshot { return indexmanager.WALTailSnapshot{} }
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
		r.targets[idx.VChannel()] = &target{idx: idx, pkFieldID: f.fieldID, pkDataType: f.dataType, checkpointTimeTick: cfg.Metas[idx.VChannel()].GetCollectionInfo().GetPkIndex().GetCheckpointTimeTick()}
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
func (r *recovery) openIndex(ctx context.Context, vchannel string, collectionID int64, pkIndex *streamingpb.PKIndexOfVChannel, schema *schemapb.CollectionSchema) error {
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
	r.targets[vchannel] = &target{idx: idx, pkFieldID: f.fieldID, pkDataType: f.dataType, checkpointTimeTick: pkIndex.GetCheckpointTimeTick()}
	return nil
}

// Run marks the recovery barrier. It starts the handover driver, whose Start
// submits the install of every open index with a manifest; ObserveMessage keeps
// applying the messages after it. It returns the first error of the replay
// before it, which makes the WAL open fail, and an error marked
// pkroot.ErrUnavailable after Close. A second Run does nothing.
func (r *recovery) Run(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errClosed("pchannel " + r.info.Name)
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
// pchannel, which cancels the install tasks and waits for them. It
// delegates corruption cleanup to each Index. A second Close does nothing.
func (r *recovery) Close(ctx context.Context) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	r.m.Close()
}

// LastAcked returns the LastAcked of the index of the pchannel, see
// indexmanager.PChannel.LastAcked.
func (r *recovery) LastAcked() uint64 { return r.m.LastAcked() }

// PChannel returns the index of the pchannel. The WAL recovery puts it in the
// recovery snapshot for the interceptor, which uses it and does not own it.
func (r *recovery) PChannel() indexmanager.PChannel { return r.m }

// retryOptions returns the retry options for transient body reads.
func (r *recovery) retryOptions() []retry.Option {
	p := indexmanager.ParamsFromConfig()
	return []retry.Option{retry.AttemptAlways(), retry.Sleep(p.BackoffBase), retry.MaxSleepTime(p.MaxBackoff)}
}

// stateOf reads the current vchannel state, or reports that none is known.
func (r *recovery) stateOf(name string) (vchannel.PKIndexState, bool) {
	if r.pkIndexState == nil {
		return vchannel.PKIndexState{}, false
	}
	return r.pkIndexState(name)
}
