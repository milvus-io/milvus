package deletebuffer

import "github.com/milvus-io/milvus/pkg/v2/util/typeutil"

// deleteBuffer caches L0 delete buffer for remote segments.
type deleteBuffer struct {
	// timestamp => DeleteData
	cache *typeutil.SkipList[uint64, []BufferItem]
}

// Cache delete data.
func (b *deleteBuffer) Cache(timestamp uint64, data []BufferItem) {
	b.cache.Upsert(timestamp, data)
}

func (b *deleteBuffer) List(since uint64) [][]BufferItem {
	return b.cache.ListAfter(since, false)
}

func (b *deleteBuffer) TruncateBefore(ts uint64) {
	b.cache.TruncateBefore(ts)
}

func NewDeleteBuffer() *deleteBuffer {
	cache, _ := typeutil.NewSkipList[uint64, []BufferItem]()
	return &deleteBuffer{
		cache: cache,
	}
}
