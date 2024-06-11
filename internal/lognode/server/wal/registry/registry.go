package registry

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/adaptor"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// builders is a map of registered wal builders.
var builders typeutil.ConcurrentMap[string, wal.OpenerBuilder]

// Register registers the wal builder.
//
// NOTE: this function must only be called during initialization time (i.e. in
// an init() function), name of builder is lowercase. If multiple Builder are
// registered with the same name, panic will occur.
func RegisterBuilder(b walimpls.OpenerBuilderImpls) {
	bb := adaptor.AdaptImplsToBuilder(b)
	_, loaded := builders.GetOrInsert(bb.Name(), bb)
	if loaded {
		panic("wal builder already registered: " + b.Name())
	}
}

// MustGetBuilder returns the wal builder by name.
func MustGetBuilder(name string) wal.OpenerBuilder {
	b, ok := builders.Get(name)
	if !ok {
		panic("wal builder not found: " + name)
	}
	return b
}
