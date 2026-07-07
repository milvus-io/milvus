//go:build test
// +build test

package registry

import (
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func ResetRegisterLocalWALManager() {
	registry = syncutil.NewFuture[WALManager]()
	releaseManualFlushPreparerRegistry = syncutil.NewFuture[ReleaseManualFlushPreparer]()
}

func ResetRegisterLocalSchemaResolvers() {
	schemaResolvers = typeutil.NewConcurrentMap[string, LocalSchemaResolver]()
}
