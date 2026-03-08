//go:build test
// +build test

package registry

import "github.com/milvus-io/milvus/pkg/v2/util/syncutil"

func ResetRegisterLocalWALManager() {
	registry = syncutil.NewFuture[WALManager]()
}
