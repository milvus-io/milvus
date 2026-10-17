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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Observe replays one message the WAL recovery delivered: a complete
// transaction, an autocommit write, a DDL or a TimeTick, in time tick order
// up to the recovery barrier. It keeps the first error and ignores the
// messages after it; Finish returns the error. After Finish or Close it does
// nothing.
func (r *Recovery) Observe(ctx context.Context, msg message.ImmutableMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || r.closed || r.err != nil {
		return
	}
	if err := r.observe(ctx, msg); err != nil {
		r.err = err
		r.logger.Error(ctx, "primary key index replay failed, the WAL open fails", mlog.FieldMessage(msg), mlog.Err(err))
	}
}

// observe dispatches one message by its type. A replicated message and a
// message of the control channel are not indexed, as on the append path. A
// CreateCollection of a vchannel that Recover opened from the catalog does
// nothing. A failed DropCollection is logged and does not fail the open.
func (r *Recovery) observe(ctx context.Context, msg message.ImmutableMessage) error {
	switch msg.MessageType() {
	case message.MessageTypeTimeTick, message.MessageTypeRecoveryBarrier:
		r.m.ObserveTimeTick(msg.TimeTick())
		return nil
	case message.MessageTypeCreateCollection:
		if funcutil.IsControlChannel(msg.VChannel()) {
			return nil
		}
		if _, ok := r.targets[msg.VChannel()]; ok {
			return nil
		}
		create := message.MustAsImmutableCreateCollectionMessageV1(msg)
		body, err := create.Body(ctx)
		if err != nil {
			return errors.Wrapf(err, "read the body of the create collection of vchannel %s", msg.VChannel())
		}
		schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
		if err := r.addCollection(ctx, msg.VChannel(), create.Header().GetCollectionId(), schema); err != nil {
			return err
		}
		if t, ok := r.targets[msg.VChannel()]; ok {
			// A collection created in this term has no manifest.
			t.idx.MarkReady(ctx, false)
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
		if err := r.m.DropCollection(ctx, msg.VChannel()); err != nil {
			r.logger.Warn(ctx, "failed to drop the primary key index of a collection during the replay", mlog.FieldVChannel(msg.VChannel()), mlog.Err(err))
		}
		return nil
	case message.MessageTypeTxn:
		if msg.ReplicateHeader() != nil {
			return nil
		}
		return r.replayTxn(ctx, msg.(message.ImmutableTxnMessage))
	case message.MessageTypeInsert, message.MessageTypeDelete:
		return r.replayBody(ctx, msg, 0, msg.TimeTick())
	default:
		return nil
	}
}

// replayTxn replays the bodies of a transaction, then its commit. The bodies
// take effect at the commit time tick.
func (r *Recovery) replayTxn(ctx context.Context, txn message.ImmutableTxnMessage) error {
	commitTT := txn.Commit().TimeTick()
	txnID := int64(txn.TxnContext().TxnID)
	bodies := 0
	if err := txn.RangeOver(func(body message.ImmutableMessage) error {
		bodies++
		return r.replayBody(ctx, body, txnID, commitTT)
	}); err != nil {
		return err
	}
	t, ok := r.targets[txn.VChannel()]
	if !ok || bodies == 0 {
		return nil
	}
	return t.replay(ctx, index.ReplayWrite{Kind: authority.KindCommitTxn, TxnID: txnID, TimeTick: commitTT})
}

// replayBody replays one insert or delete. txnID is 0 for an autocommit
// write; tt is the time tick the write takes effect at.
func (r *Recovery) replayBody(ctx context.Context, msg message.ImmutableMessage, txnID int64, tt uint64) error {
	if msg.ReplicateHeader() != nil {
		return nil
	}
	t, ok := r.targets[msg.VChannel()]
	if !ok {
		return nil
	}
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insert := message.MustAsImmutableInsertMessageV1(msg)
		body, err := insert.Body(ctx)
		if err != nil {
			return errors.Wrapf(err, "read the body of an insert of vchannel %s", msg.VChannel())
		}
		keys, err := primarykey.KeysOfInsert(body, t.pkFieldID, t.pkDataType)
		if err != nil {
			return errors.Wrapf(err, "read the primary keys of an insert of vchannel %s", msg.VChannel())
		}
		var segmentID int64
		if partitions := insert.Header().GetPartitions(); len(partitions) > 0 {
			segmentID = partitions[0].GetSegmentAssignment().GetSegmentId()
		}
		return t.replay(ctx, index.ReplayWrite{Kind: authority.KindInsert, PKs: pkindex.PKsOf(keys), SegmentID: segmentID, TxnID: txnID, TimeTick: tt})
	case message.MessageTypeDelete:
		del := message.MustAsImmutableDeleteMessageV1(msg)
		body, err := del.Body(ctx)
		if err != nil {
			return errors.Wrapf(err, "read the body of a delete of vchannel %s", msg.VChannel())
		}
		if len(body.GetSerializedExprPlan()) > 0 {
			panic("a delete by expression reached the primary key index replay, the append path does not accept it")
		}
		keys, err := primarykey.KeysOfDelete(body)
		if err != nil {
			return errors.Wrapf(err, "read the primary keys of a delete of vchannel %s", msg.VChannel())
		}
		return t.replay(ctx, index.ReplayWrite{Kind: authority.KindDelete, PKs: pkindex.PKsOf(keys), TxnID: txnID, TimeTick: tt})
	default:
		return nil
	}
}

// replay applies one write to the Index of the target and names the
// vchannel in the error.
func (t *target) replay(ctx context.Context, w index.ReplayWrite) error {
	if err := t.idx.Replay(ctx, w); err != nil {
		return errors.Wrapf(err, "replay a write into the primary key index of vchannel %s", t.idx.VChannel())
	}
	return nil
}
