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

package recovery

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// ObserveMessage applies one message the WAL recovery delivered to the index:
// a complete transaction, an autocommit write, a DDL or a TimeTick, in time
// tick order. The recovery delivers the messages from the global checkpoint
// to the recovery barrier and then the live messages through the same call,
// so the replay and the live path are one. An error before Run is kept and
// Run returns it; an error after Run stops the observe, see fail. After an
// error or Close it does nothing.
//
// It returns the work of the message that waits, nil when there is none: the
// close of the index of a dropped collection. The caller runs it after it
// releases the lock of the recovery storage, before it observes the next
// message.
func (r *Recovery) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) (after func(context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.stopped || r.err != nil {
		return nil
	}
	after, err := r.observe(ctx, msg)
	if err != nil {
		r.fail(ctx, msg, err)
		return nil
	}
	return after
}

// fail handles an error of the observe of msg. Before Run the WAL open is
// still running and the error fails it. After Run a closed index or a
// canceled context means the WAL is closing, and the recovery stops: it
// applies nothing more and does not move the observed time tick, so no
// later handover covers a write it skipped. Every other error is fatal for
// this WAL: the index missed a write it can not get back, so the WAL
// becomes unavailable and reopens from the manifest and the WAL. A
// corruption error also marks the vchannel for a cold start, see wipe.
func (r *Recovery) fail(ctx context.Context, msg message.ImmutableMessage, err error) {
	class := errorClass(err)
	if errors.Is(err, pkroot.ErrCorrupted) {
		dir := index.VChannelDir(r.mgr.Root(), msg.VChannel())
		if t, ok := r.targets[msg.VChannel()]; ok {
			dir = index.Dir(r.mgr.Root(), msg.VChannel(), t.idx.IndexID())
		}
		r.wipe[msg.VChannel()] = dir
	}
	wrapped := errors.Wrapf(err, "apply %s of vchannel %s to the primary key index", msg.MessageType(), msg.VChannel())
	if !r.finished {
		r.err = wrapped
		r.logger.Error(ctx, "primary key index observe failed, the WAL open fails", mlog.FieldVChannel(msg.VChannel()), mlog.FieldMessage(msg), mlog.String("class", class), mlog.Err(err))
		return
	}
	r.stopped = true
	if class == errorClassClosed || class == errorClassCanceled {
		r.logger.Info(ctx, "primary key index observe stopped, the WAL is closing", mlog.FieldVChannel(msg.VChannel()), mlog.String("class", class), mlog.Err(err))
		return
	}
	r.logger.Error(ctx, "primary key index observe failed, the WAL becomes unavailable", mlog.FieldVChannel(msg.VChannel()), mlog.FieldMessage(msg), mlog.String("class", class), mlog.Err(err))
	metrics.StreamingNodePKIndexFatalTotal.WithLabelValues(paramtable.GetStringNodeID(), class).Inc()
	r.onFatal(wrapped)
}

// observe dispatches one message by its type. A message of the control
// channel is not indexed. A replicated message is applied like any other:
// the index of a secondary cluster follows its WAL. A CreateIndex or a
// DropIndex of the primary key index opens or drops the Index of its
// vchannel. A failed drop is logged and does not fail the open.
//
// The engine write of a message happens here, before the TimeTick after the
// message moves the observed time tick of the pchannel. A handover reads
// that tick before it rotates, so every write it covers is in the engine.
func (r *Recovery) observe(ctx context.Context, msg message.ImmutableMessage) (func(context.Context), error) {
	switch msg.MessageType() {
	case message.MessageTypeTimeTick, message.MessageTypeRecoveryBarrier:
		// Every write before the tick is in the engine by now, on this
		// goroutine. The index passes the tick to the Decider of every
		// vchannel and evaluates its handover triggers with the tail.
		r.m.ObserveTimeTick(msg.TimeTick(), r.tail())
		return nil, nil
	case message.MessageTypeManualFlush:
		// A flush of a collection is broadcast to every vchannel of the
		// collection, so the message names the vchannel it flushes. The
		// handover of the vchannel covers the flush, see ObserveFlush.
		r.m.ObserveFlush(msg.VChannel(), msg.TimeTick())
		return nil, nil
	case message.MessageTypeFlushAll:
		// A flush of the cluster is broadcast to the control channel of every
		// pchannel, so it flushes every vchannel of this pchannel.
		r.m.ObserveFlushAll(msg.TimeTick())
		return nil, nil
	case message.MessageTypeCreateIndex:
		if funcutil.IsControlChannel(msg.VChannel()) {
			return nil, nil
		}
		header := message.MustAsImmutableCreateIndexMessageV2(msg).Header()
		if header.GetIndexType() != common.PrimaryKeyIndexType {
			return nil, nil
		}
		return r.observeCreateIndex(ctx, msg.VChannel(), header, msg.TimeTick())
	case message.MessageTypeDropIndex:
		if funcutil.IsControlChannel(msg.VChannel()) {
			return nil, nil
		}
		header := message.MustAsImmutableDropIndexMessageV2(msg).Header()
		if header.GetIndexType() != common.PrimaryKeyIndexType {
			return nil, nil
		}
		return r.observeDropIndex(msg.VChannel(), header.GetIndexIds(), msg.TimeTick()), nil
	case message.MessageTypeDropCollection:
		if funcutil.IsControlChannel(msg.VChannel()) {
			return nil, nil
		}
		if _, ok := r.targets[msg.VChannel()]; !ok {
			return nil, nil
		}
		// The drop is in the WAL, so the collection is gone and the writes
		// after it are not indexed either way. The observe drops the index,
		// not the interceptor: a write before the drop in WAL order reaches
		// the observe after the append of the drop returned, and it must
		// still find the engine open. See dropIndex for the close.
		return r.dropIndex(msg.VChannel(), 0), nil
	case message.MessageTypeTxn:
		return nil, r.observeTxn(ctx, msg.(message.ImmutableTxnMessage))
	case message.MessageTypeInsert, message.MessageTypeDelete:
		t, ok := r.targets[msg.VChannel()]
		if !ok {
			return nil, nil
		}
		w, err := r.writeOf(ctx, t, msg)
		if err != nil {
			return nil, err
		}
		w.TimeTick = msg.TimeTick()
		return nil, r.m.Apply(ctx, msg.VChannel(), w)
	default:
		return nil, nil
	}
}

// observeTxn applies a committed transaction as one write, at its commit
// time tick, as on the append path, where the index decides it at the
// commit. A manifest checkpoint between a body and the commit therefore
// excludes the whole transaction, and the observe applies it whole.
//
// The deletes of every body and the puts of every body form one Write, so
// the index writes them in one batch with the deletes first: a transaction
// that the index built for an insert holds the insert and then its
// companion delete, at the same time tick, and the insert must survive.
func (r *Recovery) observeTxn(ctx context.Context, txn message.ImmutableTxnMessage) error {
	t, ok := r.targets[txn.VChannel()]
	if !ok {
		return nil
	}
	txnID := int64(txn.TxnContext().TxnID)
	w := index.Write{TimeTick: txn.Commit().TimeTick()}
	if err := txn.RangeOver(func(body message.ImmutableMessage) error {
		bw, err := r.writeOf(ctx, t, body)
		if err != nil {
			return err
		}
		w.Deletes = append(w.Deletes, bw.Deletes...)
		w.Puts = append(w.Puts, bw.Puts...)
		return nil
	}); err != nil {
		return errors.Wrapf(err, "txn %d", txnID)
	}
	return r.m.Apply(ctx, txn.VChannel(), w)
}

// observeCreateIndex enables or does not enable the primary key index of a
// vchannel at its CreateIndex at tt. The vchannel module of the WAL recovery
// observed the message before, so the vchannel state holds the decision at
// tt: the index is enabled when the state names this index at tt.
//
// When the index is enabled, the observe opens its Index or finds the one
// that the interceptor opened at the append, or that Recover opened from the
// catalog meta. An Index opened here is ready at once: an index enabled after
// the checkpoint of the pchannel has no manifest. When the index is not
// enabled, the observe drops an Index of this index ID that the interceptor
// opened at the append. The returned function closes a dropped Index, see
// dropIndex.
func (r *Recovery) observeCreateIndex(ctx context.Context, vchannel string, header *message.CreateIndexMessageHeader, tt uint64) (func(context.Context), error) {
	state, ok := r.m.VChannelState(vchannel)
	pk := state.PKIndex
	enabled := ok && pk.GetIndexId() == header.GetIndexId() && pk.GetEnableTimeTick() == tt
	if t, ok := r.targets[vchannel]; ok {
		if t.idx.IndexID() == header.GetIndexId() || t.idx.EnableTimeTick() > tt {
			// the index is open already, or a replay reached the CreateIndex
			// of an index that a later index replaced
			return nil, nil
		}
	}
	if !enabled {
		return r.dropIndex(vchannel, header.GetIndexId()), nil
	}
	var after func(context.Context)
	if t, ok := r.targets[vchannel]; ok {
		// the DropIndex of the earlier index is before tt and dropped it, so
		// this does not happen; drop it anyway so that the open succeeds
		after = r.dropIndex(vchannel, t.idx.IndexID())
	}
	if err := r.openIndex(ctx, vchannel, header.GetCollectionId(), pk, state.Schema); err != nil {
		return after, err
	}
	r.targets[vchannel].idx.MarkReady(ctx)
	return after, nil
}

// observeDropIndex drops the Index of a vchannel when the DropIndex at tt
// names its index ID, or names no index. A DropIndex at or before the
// CreateIndex of the open Index is the replay of a drop of an earlier index
// and does nothing. The returned function closes the dropped Index, see
// dropIndex.
func (r *Recovery) observeDropIndex(vchannel string, indexIDs []int64, tt uint64) func(context.Context) {
	t, ok := r.targets[vchannel]
	if !ok || t.idx.EnableTimeTick() >= tt {
		return nil
	}
	if len(indexIDs) > 0 && !slices.Contains(indexIDs, t.idx.IndexID()) {
		return nil
	}
	return r.dropIndex(vchannel, t.idx.IndexID())
}

// dropIndex forgets the target of a vchannel and detaches the Index of
// indexID of the vchannel at once, so the writes after the drop are not
// applied and a later CreateIndex can open a new Index. indexID 0 matches any
// Index, the drop of the collection. It returns nil when no Index matches.
// Otherwise it returns a function that closes the Index and removes its local
// files. The close waits for a running handover, an upload, so the recovery
// storage runs the function after it releases its lock, and before it
// observes the next message. A failed drop is logged: the index is gone
// either way.
func (r *Recovery) dropIndex(vchannel string, indexID int64) func(context.Context) {
	if t, ok := r.targets[vchannel]; ok && (indexID == 0 || t.idx.IndexID() == indexID) {
		delete(r.targets, vchannel)
	}
	if !r.m.DetachIndex(vchannel, indexID) {
		return nil
	}
	return func(ctx context.Context) {
		if err := r.m.DropIndex(ctx, vchannel, indexID); err != nil {
			r.logger.Warn(ctx, "failed to drop the primary key index", mlog.FieldVChannel(vchannel), mlog.Int64("indexID", indexID), mlog.Err(err))
		}
	}
}

// writeOf translates an insert or a delete into index terms. A message of
// another type gives an empty write. The caller sets the time tick.
func (r *Recovery) writeOf(ctx context.Context, t *target, msg message.ImmutableMessage) (index.Write, error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insert := message.MustAsImmutableInsertMessageV1(msg)
		body, err := readBody(ctx, r, "insert", insert.Body)
		if err != nil {
			return index.Write{}, errors.Wrap(err, "read the body of an insert")
		}
		keys, err := primarykey.KeysOfInsert(body, t.pkFieldID, t.pkDataType)
		if err != nil {
			// The append path read the same keys from the same body before it
			// wrote the message, so a body whose keys can not be read is
			// storage corruption, like a body that does not unmarshal.
			panic(fmt.Sprintf("the primary keys of a persisted insert of vchannel %s can not be read in the primary key index observe: %v", msg.VChannel(), err))
		}
		var segmentID int64
		if partitions := insert.Header().GetPartitions(); len(partitions) > 0 {
			segmentID = partitions[0].GetSegmentAssignment().GetSegmentId()
		}
		puts := make([]index.Put, 0, len(keys.Int64Values)+len(keys.StringValues))
		for _, pk := range pkindex.PKsOf(keys) {
			puts = append(puts, index.Put{PK: pk, SegmentID: segmentID})
		}
		return index.Write{Puts: puts}, nil
	case message.MessageTypeDelete:
		del := message.MustAsImmutableDeleteMessageV1(msg)
		body, err := readBody(ctx, r, "delete", del.Body)
		if err != nil {
			return index.Write{}, errors.Wrap(err, "read the body of a delete")
		}
		if len(body.GetSerializedExprPlan()) > 0 {
			panic("a delete by expression reached the primary key index observe, the append path does not accept it")
		}
		keys, err := primarykey.KeysOfDelete(body)
		if err != nil {
			panic(fmt.Sprintf("the primary keys of a persisted delete of vchannel %s can not be read in the primary key index observe: %v", msg.VChannel(), err))
		}
		return index.Write{Deletes: pkindex.PKsOf(keys)}, nil
	default:
		return index.Write{}, nil
	}
}

// readBody reads the body of a persisted message with read. A body that
// does not unmarshal is storage corruption: the append path rejects such a
// body before it is written (wal/utility/message_body.go), and the other
// modules of the recovery storage read a persisted body with MustBody. This
// follows the same rule and ends the process; a WAL that reopened would only
// read the same body again. Every other error, such as a cipher whose key
// service is away, is retried with backoff until ctx ends. The body cache
// does not retry: a failed decode is not cached, and every call decodes
// again. The retry runs on the observe goroutine of the recovery storage,
// under its lock, so the whole pchannel waits with it.
func readBody[B proto.Message](ctx context.Context, r *Recovery, what string, read func(context.Context) (B, error)) (B, error) {
	var body B
	err := retry.Do(ctx, func() error {
		b, err := read(ctx)
		if errors.Is(err, message.ErrMalformedBody) {
			panic(fmt.Sprintf("malformed body of a persisted %s message in the primary key index observe: %v", what, err))
		}
		body = b
		return err
	}, r.retryOptions()...)
	return body, err
}
