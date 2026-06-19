package allocator

import "github.com/milvus-io/milvus/pkg/v3/util/typeutil"

type Allocator interface {
	AllocID() (typeutil.UniqueID, error)
}
