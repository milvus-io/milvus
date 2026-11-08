package contextutil

import (
	"context"
	"sync/atomic"
)

type (
	pickResultKeyType int
)

var pickResultServerIDKey pickResultKeyType = 0

// UnspecifiedServerID asks the picker to select any ready server.
const UnspecifiedServerID int64 = 0

// WithPickServerID creates routing and result state for one RPC/stream. Callers
// create it immediately before dispatch, including each assignment retry.
func WithPickServerID(ctx context.Context, serverID int64) context.Context {
	return context.WithValue(ctx, pickResultServerIDKey, &serverIDPickResult{
		requestedServerID: serverID,
	})
}

// EnsurePickServerID reuses the state supplied for this RPC, or installs an
// unspecified route when the caller did not supply one. Presence must be checked
// separately from the ID: an existing unspecified route also needs no allocation.
func EnsurePickServerID(ctx context.Context) context.Context {
	if _, ok := ctx.Value(pickResultServerIDKey).(*serverIDPickResult); ok {
		return ctx
	}
	return WithPickServerID(ctx, UnspecifiedServerID)
}

// GetPickServerID returns the requested server, or the actual selection when the
// request left it unspecified. False means the picker should use round-robin.
func GetPickServerID(ctx context.Context) (int64, bool) {
	pr, ok := ctx.Value(pickResultServerIDKey).(*serverIDPickResult)
	if !ok {
		return UnspecifiedServerID, false
	}
	serverID := pr.requestedServerID
	if serverID == UnspecifiedServerID {
		serverID = pr.pickedServerID.Load()
	}
	return serverID, serverID != UnspecifiedServerID
}

// SetPickedServerID publishes the actual selected server. At the start of every
// pick, clear it with UnspecifiedServerID: automatic routing must remain free to
// choose another server on retry, and failed picks must not retain stale results.
func SetPickedServerID(ctx context.Context, serverID int64) {
	if pr, ok := ctx.Value(pickResultServerIDKey).(*serverIDPickResult); ok {
		pr.pickedServerID.Store(serverID)
	}
}

// GetPickedServerID returns only a completed pick, never an unfulfilled routing
// request. This distinction prevents attributing pre-connection failures to a peer.
func GetPickedServerID(ctx context.Context) int64 {
	if pr, ok := ctx.Value(pickResultServerIDKey).(*serverIDPickResult); ok {
		return pr.pickedServerID.Load()
	}
	return UnspecifiedServerID
}

// serverIDPickResult owns both routing input and diagnostic output. The requested
// ID is immutable; the actual ID is atomic because streams may finish concurrently
// with a pick. No resolver address or connection metadata is retained.
type serverIDPickResult struct {
	requestedServerID int64
	pickedServerID    atomic.Int64
}

// WithFailFastPick asks an explicitly routed query to return an unready
// connection to its replica orchestrator instead of queuing in the picker.
func WithFailFastPick(ctx context.Context) context.Context {
	return context.WithValue(ctx, failFastPickKey{}, true)
}

type failFastPickKey struct{}

func IsFailFastPick(ctx context.Context) bool {
	enabled, _ := ctx.Value(failFastPickKey{}).(bool)
	return enabled
}
