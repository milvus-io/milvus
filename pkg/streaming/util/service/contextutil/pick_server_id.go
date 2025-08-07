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
