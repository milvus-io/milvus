//go:build test
// +build test

package registry

import "github.com/milvus-io/milvus/pkg/util/syncutil"

func ResetRegistration() {
	localRegistry = make(map[AppendOperatorType]*syncutil.Future[AppendOperator])
	localRegistry[AppendOperatorTypeMsgstream] = syncutil.NewFuture[AppendOperator]()
	localRegistry[AppendOperatorTypeStreaming] = syncutil.NewFuture[AppendOperator]()
}
