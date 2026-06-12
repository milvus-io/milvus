package mlog

import "context"

// WithReqID returns a context with request ID attached to logging fields.
func WithReqID(ctx context.Context, reqID int64) context.Context {
	return WithFields(ctx, Int64("req_id", reqID))
}

// WithModule returns a context with module name attached to logging fields.
func WithModule(ctx context.Context, module string) context.Context {
	return WithFields(ctx, FieldModule(module))
}
