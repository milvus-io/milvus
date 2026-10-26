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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const interceptorName = "pkindex"

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates the builder of the primary key index interceptor.
// The interceptor must be placed between the replicate and the timetick interceptor.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

// collectionSchemaLister is the part of the shard manager that the interceptor
// needs to learn the collections that already exist when the WAL opens.
type collectionSchemaLister interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

// Build returns the passthrough interceptor when the recovery snapshot
// carries no index. The WAL recovery owns the index of the pchannel: it
// creates it before the interceptors are built, hands it over in the
// snapshot, and closes it with the WAL. The interceptor only uses it. The
// streaming node builds the index manager at start exactly when
// streaming.pkindex.enabled is true, so no index in the snapshot means the
// index is off or the recovery did not build one.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	ctx := context.TODO()
	if param.InitialRecoverSnapshot == nil || param.InitialRecoverSnapshot.PKIndex == nil {
		return passthroughInterceptor{}
	}
	ch := param.InitialRecoverSnapshot.PKIndex
	onFatal := onFatalOf(param)
	reg := newRegistry(ch)
	// TODO: GetAllCollectionSchemaInfos skips a collection whose schema the
	// shard manager does not hold, so that collection gets no index and its
	// writes pass through.
	if lister, ok := param.ShardManager.(collectionSchemaLister); ok {
		for collectionID, info := range lister.GetAllCollectionSchemaInfos() {
			_, err := reg.add(ctx, info.VChannel, collectionID, info.Schema)
			if err == nil {
				continue
			}
			// The WAL can not serve the vchannel without its index. A local disk
			// error makes the WAL reject all appends, and the next open of the WAL
			// tries again. It does not stop the other WALs of the node.
			if errors.Is(err, errOpenIndex) {
				onFatal(err)
				break
			}
			// A collection that exists can not have a schema the index rejects.
			panic(fmt.Sprintf("create the primary key index of vchannel %s at wal open: %v", info.VChannel, err))
		}
	}
	// param.TxnManager is a pointer, so it must not be assigned to the interface
	// while it is nil. The interface would then be non-nil and carry a nil pointer.
	var sessions txnSessions
	if param.TxnManager != nil {
		sessions = param.TxnManager
	}
	if snapshot := param.InitialRecoverSnapshot; snapshot.TxnBuffer != nil {
		if err := restorePending(ctx, reg, ch, snapshot.TxnBuffer); err != nil {
			// The commit of an open transaction would be decided without its
			// earlier bodies, so the WAL can not serve. The next open tries again.
			onFatal(errors.Wrap(err, "restore the pending writes of the primary key index"))
		}
	}
	return &appendInterceptor{
		ch:       ch,
		registry: reg,
		sessions: sessions,
		metrics:  newMetrics(),
		onFatal:  onFatal,
	}
}

// restorePending feeds the bodies of the transactions that were open at the
// recovery barrier to the index of their vchannel, so that the CommitTxn of
// such a transaction is decided over all of its bodies. The bodies are in
// the WAL already: they need no in-flight entry, and the observe of the
// recovery applies them with the commit. A body that does not unmarshal, or
// whose keys can not be read, ends the process, like a persisted body does
// everywhere in the recovery.
func restorePending(ctx context.Context, reg *registry, ch *snpkindex.PChannelIndexManager, buf *utility.TxnBuffer) error {
	for txnID, builder := range buf.GetUncommittedMessageBuilder() {
		_, bodies := builder.Messages()
		for _, body := range bodies {
			t := reg.get(body.VChannel())
			if t == nil {
				continue
			}
			a := ch.Decider(body.VChannel())
			if a == nil {
				continue
			}
			// The commit of the transaction encrypts its companion delete
			// with the cipher config of its bodies.
			t.rememberCipher(body)
			switch body.MessageType() {
			case message.MessageTypeInsert:
				b, err := message.MustAsImmutableInsertMessageV1(body).Body(ctx)
				if err != nil {
					return malformedOr(err, "insert", body)
				}
				keys, err := primarykey.KeysOfInsert(b, t.pkFieldID, t.pkDataType)
				if err != nil {
					panic(fmt.Sprintf("the primary keys of a persisted insert of transaction %d can not be read: %v", txnID, err))
				}
				a.RestorePending(int64(txnID), dedup.KindInsert, snpkindex.PKsOf(keys))
			case message.MessageTypeDelete:
				b, err := message.MustAsImmutableDeleteMessageV1(body).Body(ctx)
				if err != nil {
					return malformedOr(err, "delete", body)
				}
				keys, err := primarykey.KeysOfDelete(b)
				if err != nil {
					panic(fmt.Sprintf("the primary keys of a persisted delete of transaction %d can not be read: %v", txnID, err))
				}
				a.RestorePending(int64(txnID), dedup.KindDelete, snpkindex.PKsOf(keys))
			}
		}
	}
	return nil
}

// malformedOr ends the process on a persisted body that does not unmarshal,
// the rule of every module of the recovery storage, and returns any other
// read error with its context.
func malformedOr(err error, what string, body message.ImmutableMessage) error {
	if errors.Is(err, message.ErrMalformedBody) {
		panic(fmt.Sprintf("malformed body of a persisted %s message %s: %v", what, body.MessageID(), err))
	}
	return errors.Wrapf(err, "read the body of a %s of vchannel %s", what, body.VChannel())
}

// onFatalOf returns param.OnFatal. When it is nil, as in tests, the returned
// function only logs the error.
func onFatalOf(param *interceptors.InterceptorBuildParam) func(error) {
	if param.OnFatal != nil {
		return param.OnFatal
	}
	pchannel := param.ChannelInfo.Name
	return func(err error) {
		mlog.Error(context.TODO(), "primary key index failed, no handler marks the WAL unavailable",
			mlog.FieldPChannel(pchannel), mlog.Err(err))
	}
}

// passthroughInterceptor is used when the streaming node has no index manager.
type passthroughInterceptor struct{}

func (passthroughInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	return appendOp(ctx, msg)
}

func (passthroughInterceptor) Close() {}
