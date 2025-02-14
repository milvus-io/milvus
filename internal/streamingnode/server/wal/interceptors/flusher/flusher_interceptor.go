package flusher

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var (
	_ interceptors.Interceptor                  = (*flusherAppendInterceptor)(nil)
	_ interceptors.InterceptorWithGracefulClose = (*flusherAppendInterceptor)(nil)
)

// flusherAppendInterceptor is an append interceptor to handle the append operation from consumer.
// the flusher is a unique consumer that will consume the message from wal.
// It will handle the message and persist the message other storage from wal.
type flusherAppendInterceptor struct {
	flusher *flusherimpl.WALFlusherImpl
}

func (c *flusherAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	// TODO: The interceptor will also do some slow down for streaming service if the consumer is lag too much.
	return append(ctx, msg)
}

// GracefulClose will close the flusher gracefully.
func (c *flusherAppendInterceptor) GracefulClose() {
	c.flusher.Close()
}

func (c *flusherAppendInterceptor) Close() {}
