package shard

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type splitTargetGate struct {
	collection int64
	source     string
	opened     atomic.Bool
}

func (impl *shardInterceptor) addSplitTargetGate(channel string, collection int64, source string) {
	impl.splitGateMu.Lock()
	defer impl.splitGateMu.Unlock()
	if impl.splitGates == nil {
		impl.splitGates = make(map[string]*splitTargetGate)
	}
	if impl.splitGates[channel] == nil {
		impl.splitGates[channel] = &splitTargetGate{collection: collection, source: source}
	}
}

// The gate is cluster-local admission, never part of replication. A promoted
// secondary starts checking its local serving generation on its first DML.
func (impl *shardInterceptor) checkSplitTargetAdmission(ctx context.Context, msg message.MutableMessage) error {
	if msg.ReplicateHeader() != nil {
		return nil
	}
	switch msg.MessageType() {
	case message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeBeginTxn:
	default:
		return nil
	}
	impl.splitGateMu.Lock()
	gate := impl.splitGates[msg.VChannel()]
	impl.splitGateMu.Unlock()
	if gate == nil || gate.opened.Load() {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	allowed, err := querySplitTargetAdmission(probeCtx, gate.collection, gate.source, msg.VChannel())
	if err == nil && allowed {
		gate.opened.Store(true)
		return nil
	}
	if msg.MessageType() == message.MessageTypeBeginTxn && impl.txnManager != nil && msg.TxnContext() != nil {
		impl.txnManager.FailTxn(msg.TxnContext().TxnID)
	}
	return status.NewShardFenced(msg.VChannel())
}

func querySplitTargetAdmission(ctx context.Context, collection int64, source, target string) (bool, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return false, err
	}
	response, err := coord.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{
		CollectionID: collection, SplitAdmissionOnly: true, SplitSource: source, SplitTarget: target,
	})
	if err = merr.CheckRPCCall(response, err); err != nil {
		return false, err
	}
	return response.GetSplitAdmissionAllowed(), nil
}
