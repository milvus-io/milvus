package broadcaster

import (
	"context"

	"go.opentelemetry.io/otel/codes"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type broadcasterWithRK struct {
	broadcaster *broadcastTaskManager
	broadcastID uint64
	guards      *lockGuards
}

func (b *broadcasterWithRK) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// The idempotency decision lives in the manager, under the same lock that
	// registers the task: see getOrAddBroadcastTask. It used to live here, as a
	// lookup separate from the registration, with the resource keys this object
	// holds expected to keep two same-key requests apart in between. They do not,
	// whenever the lock names a different object than the scope does.
	//
	// Read the refreshable bound here so the manager never reads config under its
	// lock.
	keyLengthLimit := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.GetAsInt()

	// Stamping the header, opening the span and injecting the trace context all
	// operate on this call's own values, so they stay outside the manager lock.
	// Keep a trace context in the broadcast message so that the DDL ack callback
	// can still extract it after the original caller span is long gone.
	msg = msg.OverwriteBroadcastHeader(b.broadcastID, b.guards.ResourceKeys()...)
	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALBroadcast)
	defer span.End()
	message.InjectTraceContext(ctx, msg)

	result, dup, guardsTransferred, err := b.broadcaster.broadcast(ctx, msg, b.broadcastID, b.guards, keyLengthLimit)
	if guardsTransferred {
		// The registered task owns the guards now and releases them from its ack
		// callback, on another goroutine; drop ours to avoid a double unlock. Checked
		// before err, not after: a failure to wait for the acks does not hand the
		// guards back, because the task goes on broadcasting without this request.
		b.guards = nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if dup != nil {
		// Nothing was registered and nothing reached the WAL, so the guards are
		// still ours: leave them for the caller's deferred Close().
		dupMsg, results := dup.BroadcastResultIfAcked()
		return &types.BroadcastAppendResult{
			BroadcastID:   dupMsg.BroadcastHeader().BroadcastID,
			AppendResults: results,
			Duplicated:    dupMsg,
		}, nil
	}
	return result, nil
}

func (b *broadcasterWithRK) Close() {
	if b.guards != nil {
		b.guards.Unlock()
	}
}
