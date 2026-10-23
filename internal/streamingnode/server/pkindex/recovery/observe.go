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

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// ObserveMessage applies one message the WAL recovery delivered to the index:
// a complete transaction, an autocommit write, a DDL or a TimeTick, in time
// tick order. The recovery delivers the messages from the global checkpoint
// to the recovery barrier and then the live messages through the same call,
// so the replay and the live path are one. An error before Run is kept and
// Run returns it; an error after Run stops the observe, see fail. After an
// error or Close it does nothing.
func (r *Recovery) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.stopped || r.err != nil {
		return
	}
	if err := r.observe(ctx, msg); err != nil {
		r.fail(ctx, msg, err)
	}
}

// observe dispatches one message by its type. A message of the control
// channel is not indexed. A replicated message is applied like any other:
// the index of a secondary cluster follows its WAL. A CreateCollection of a
// vchannel that Recover opened from the catalog does nothing. A failed
// DropCollection is logged and does not fail the open.
//
// The engine write of a message happens here, before the TimeTick after the
// message moves the observed time tick of the pchannel. A handover reads
// that tick before it rotates, so every write it covers is in the engine.
func (r *Recovery) observe(ctx context.Context, msg message.ImmutableMessage) error {
	switch msg.MessageType() {
	case message.MessageTypeTimeTick, message.MessageTypeRecoveryBarrier:
		// Every write before the tick is in the engine by now, on this
		// goroutine. The index evaluates its handover triggers with the tail.
		r.m.ObserveTimeTick(msg.TimeTick(), r.tail())
		// A vchannel that is degraded until a time tick, or that holds writes
		// with an unknown outcome, learns of the tick too. The others do not:
		// a TimeTick does not fan out to every idle vchannel.
		for _, t := range r.targets {
			if a := t.idx.Authority(); a.WantsTimeTick() {
				a.ObserveTimeTick(msg.TimeTick())
			}
		}
		return nil
	case message.MessageTypeManualFlush:
		// A flush of a collection is broadcast to every vchannel of the
		// collection, so the message names the vchannel it flushes. The
		// handover of the vchannel covers the flush, see RequestHandover.
		r.m.RequestHandover(msg.VChannel(), msg.TimeTick())
		return nil
	case message.MessageTypeFlushAll:
		// A flush of the cluster is broadcast to the control channel of every
		// pchannel, so it flushes every vchannel of this pchannel.
		r.m.RequestHandoverAll(msg.TimeTick())
		return nil
	case message.MessageTypeCreateCollection:
		// TODO: the enabling message of the index is a CreateIndex of type
		// PRIMARY_KEY, not the CreateCollection. The CreateIndex message does
		// not carry the index type yet, and it is broadcast to the control
		// channel only, so the index of a collection is opened at its
		// CreateCollection until that message reaches the data vchannels.
		if funcutil.IsControlChannel(msg.VChannel()) {
			return nil
		}
		if _, ok := r.targets[msg.VChannel()]; ok {
			// The interceptor opens the index at the append, and the observe
			// finds it: the open is get-or-create on both sides.
			return nil
		}
		create := message.MustAsImmutableCreateCollectionMessageV1(msg)
		body, err := readBody(ctx, r, "create collection", create.Body)
		if err != nil {
			return errors.Wrap(err, "read the body of the create collection")
		}
		schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
		if err := r.addCollection(ctx, msg.VChannel(), create.Header().GetCollectionId(), schema); err != nil {
			return err
		}
		if t, ok := r.targets[msg.VChannel()]; ok {
			// A collection created after the checkpoint has no manifest, so its
			// committed set is empty and the Index is ready at once, see
			// Index.MarkReady.
			t.idx.MarkReady(ctx)
		}
		return nil
	case message.MessageTypeDropCollection:
		if funcutil.IsControlChannel(msg.VChannel()) {
			return nil
		}
		if _, ok := r.targets[msg.VChannel()]; !ok {
			return nil
		}
		delete(r.targets, msg.VChannel())
		// The drop is in the WAL, so the collection is gone and the writes
		// after it are not indexed either way. The observe closes the index,
		// not the interceptor: a write before the drop in WAL order reaches
		// the observe after the append of the drop returned, and it must
		// still find the engine open. The close waits for a running handover,
		// an upload, so it runs outside the lock of the recovery storage, as
		// a task.
		r.handles = append(r.handles, r.sched.Submit(&dropTask{m: r.m, vchannel: msg.VChannel(), logger: r.logger}))
		return nil
	case message.MessageTypeTxn:
		return r.observeTxn(ctx, msg.(message.ImmutableTxnMessage))
	case message.MessageTypeInsert, message.MessageTypeDelete:
		t, ok := r.targets[msg.VChannel()]
		if !ok {
			return nil
		}
		w, err := r.writeOf(ctx, t, msg)
		if err != nil {
			return err
		}
		w.TimeTick = msg.TimeTick()
		return r.m.Apply(ctx, msg.VChannel(), w)
	default:
		return nil
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

// dropTask closes the index of a dropped collection and removes its local
// files, outside the lock of the recovery storage.
type dropTask struct {
	m        *pkindex.PChannelIndexManager
	vchannel string
	logger   *mlog.Logger
}

// Execute implements nodescheduler.Task. A failed drop is logged, as on the
// append path: the collection is gone either way.
func (t *dropTask) Execute(ctx context.Context) error {
	if err := t.m.DropCollection(ctx, t.vchannel); err != nil {
		t.logger.Warn(ctx, "failed to drop the primary key index of a dropped collection", mlog.FieldVChannel(t.vchannel), mlog.Err(err))
	}
	return nil
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
