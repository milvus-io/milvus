package registry

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

type AppendOperatorType int

const (
	AppendOperatorTypeMsgstream AppendOperatorType = iota + 1
	AppendOperatorTypeStreaming
)

var localRegistry = make(map[AppendOperatorType]*syncutil.Future[AppendOperator])

// AppendOperator is used to append messages, there's only two implement of this interface:
// 1. streaming.WAL()
// 2. old msgstream interface
type AppendOperator interface {
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses
}

func init() {
	localRegistry[AppendOperatorTypeMsgstream] = syncutil.NewFuture[AppendOperator]()
	localRegistry[AppendOperatorTypeStreaming] = syncutil.NewFuture[AppendOperator]()
}

func Register(typ AppendOperatorType, op AppendOperator) {
	localRegistry[typ].Set(op)
}

func GetAppendOperator() *syncutil.Future[AppendOperator] {
	if streamingutil.IsStreamingServiceEnabled() {
		return localRegistry[AppendOperatorTypeStreaming]
	}
	return localRegistry[AppendOperatorTypeMsgstream]
}
