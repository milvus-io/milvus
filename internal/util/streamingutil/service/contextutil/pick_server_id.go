package contextutil

import (
	"context"
)

type (
	pickResultKeyType int
)

var pickResultServerIDKey pickResultKeyType = 0

// WithPickServerID returns a new context with the pick result.
func WithPickServerID(ctx context.Context, serverID int64) context.Context {
	return context.WithValue(ctx, pickResultServerIDKey, &serverIDPickResult{
		serverID: serverID,
	})
}

// GetPickServerID must get the pick result from context.
// panic otherwise.
func GetPickServerID(ctx context.Context) (int64, bool) {
	pr := ctx.Value(pickResultServerIDKey)
	if pr == nil {
		return -1, false
	}
	return pr.(*serverIDPickResult).serverID, true
}

// serverIDPickResult is used to store the result of picker.
type serverIDPickResult struct {
	serverID int64
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
