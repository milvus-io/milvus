package registry

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// builders is a map of registered wal builders.
var builders typeutil.ConcurrentMap[string, walimpls.OpenerBuilderImpls]

// Register registers the wal builder.
//
// NOTE: this function must only be called during initialization time (i.e. in
// an init() function), name of builder is lowercase. If multiple Builder are
// registered with the same name, panic will occur.
func RegisterBuilder(b walimpls.OpenerBuilderImpls) {
	_, loaded := builders.GetOrInsert(b.Name(), b)
	if loaded {
		panic("walimpls builder already registered: " + b.Name())
	}
}

// MustGetBuilder returns the walimpls builder by name.
func MustGetBuilder(name string) walimpls.OpenerBuilderImpls {
	b, ok := builders.Get(name)
	if !ok {
		panic("walimpls builder not found: " + name)
	}
	return b
}
