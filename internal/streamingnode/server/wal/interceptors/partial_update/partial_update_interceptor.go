package partial_update

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	interceptorName = "pk_state"
)

// IsPKStateConflict reports whether err originated from a CAS conflict
// raised by this interceptor. Safe to call after the error has crossed the
// WAL append RPC boundary (StreamingClient re-hydrates StreamingError).
func IsPKStateConflict(err error) bool {
	if err == nil {
		return false
	}
	se := status.AsStreamingError(err)
	if se == nil {
		return false
	}
	return se.IsPKStateConflict()
}

// newConflictError wraps the underlying CAS-mismatch reason into a
// StreamingError that survives the WAL append RPC round trip.
func newConflictError(reason string) error {
	return status.NewInner("%s%s", status.PKStateConflictCausePrefix, reason)
}

var (
	_ interceptors.Interceptor            = (*pkStateInterceptor)(nil)
	_ interceptors.InterceptorWithMetrics = (*pkStateInterceptor)(nil)
	_ interceptors.InterceptorWithReady   = (*pkStateInterceptor)(nil)
)

// pkStateInterceptor enforces optimistic concurrency control for
// pk-state CAS upserts at the WAL append boundary.
type pkStateInterceptor struct {
	cache *PKVersionCache
	ready chan struct{}
}

func (p *pkStateInterceptor) Name() string { return interceptorName }

func (p *pkStateInterceptor) Ready() <-chan struct{} { return p.ready }

func (p *pkStateInterceptor) Close() { p.cache.Close() }

// DoAppend performs the CAS check for pk-state inserts. For all other
// messages it is a transparent pass-through.
func (p *pkStateInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	if msg.MessageType() != message.MessageTypeInsert {
		return appendOp(ctx, msg)
	}
	im := message.MustAsMutableInsertMessageV1(msg)
	h := im.Header()
	if h.GetOccMode() != messagespb.OCCMode_OCC_MODE_CAS {
		return appendOp(ctx, msg)
	}

	// Wait until interceptor is ready (CAS state is recovered).
	select {
	case <-p.ready:
	default:
		select {
		case <-p.ready:
		case <-ctx.Done():
			return nil, status.NewOnShutdownError("pk-state interceptor not ready, leader switching")
		}
	}

	expectedPks := h.GetExpectedPks()
	expectedTs := h.GetExpectedRowTimestamps()
	expectedExists := h.GetExpectedRowExists()
	pkSize := typeutil.GetSizeOfIDs(expectedPks)
	if pkSize != len(expectedTs) || pkSize != len(expectedExists) {
		return nil, status.NewUnrecoverableError("pk-state OCC header malformed")
	}

	collectionID := h.GetCollectionId()
	keys := make([]pkKey, 0, pkSize)
	for i := 0; i < pkSize; i++ {
		pk := typeutil.GetPK(expectedPks, int64(i))
		keys = append(keys, pkKey{collectionID: collectionID, pk: pk})
	}

	unlock := p.cache.Lock(keys)
	// Check phase.
	for i, key := range keys {
		if err := p.cache.Check(key, expectedTs[i], expectedExists[i]); err != nil {
			unlock()
			return nil, newConflictError(err.Error())
		}
	}
	// Append.
	msgID, err := appendOp(ctx, msg)
	if err != nil {
		unlock()
		return nil, err
	}
	// Update cache with the assigned TimeTick. Timetick interceptor runs
	// before pk_state in the chain, so msg.TimeTick() is set.
	ts := msg.TimeTick()
	for _, key := range keys {
		p.cache.Update(key, ts)
	}
	unlock()
	return msgID, nil
}
