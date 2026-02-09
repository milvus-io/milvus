// Package mlog provides a context-aware logging library built on zap.
//
// It enforces context usage in all log operations and supports scoped field
// attachment via context. Context fields accumulate across WithFields calls,
// allowing child contexts to inherit parent fields.
//
// Basic usage:
//
//	ctx := context.Background()
//	mlog.Info(ctx, "request received", mlog.String("path", "/api/search"))
//
// Node-level initialization (once per process):
//
//	logger, _ := zap.NewProduction()
//	mlog.InitNode(logger, nodeId) // nodeId included in all log entries
//
// Scoped context logging:
//
//	ctx = mlog.WithFields(ctx, mlog.String("request_id", "abc123"))
//	ctx = mlog.WithFields(ctx, mlog.Int64("user_id", 42))
//	mlog.Info(ctx, "processing") // includes both request_id and user_id
//
// Cross-node field propagation via gRPC:
//
//	// Attach fields that propagate across RPC calls
//	ctx = mlog.WithFields(ctx,
//	    mlog.FieldCollectionName("my_collection", mlog.OptPropagated()),
//	    mlog.FieldCollectionID(12345, mlog.OptPropagated()),
//	)
//
//	// Use gRPC interceptors to automatically propagate fields
//	// Server side: mlog.UnaryServerInterceptor("modulename")
//	// Client side: mlog.UnaryClientInterceptor()
//
// Runtime log level changes:
//
//	mlog.SetLevel(mlog.DebugLevel)
//
// Custom logger initialization:
//
//	logger, _ := zap.NewDevelopment()
//	mlog.Init(logger)
package mlog
