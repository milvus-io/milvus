package deletebuffer

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/storage"
)

// Item wraps cache item as `timed`.
type Item struct {
	Ts   uint64
	Data []BufferItem
}

// Timestamp implements `timed`.
func (item *Item) Timestamp() uint64 {
	return item.Ts
}

// Size implements `timed`.
func (item *Item) Size() int64 {
	return lo.Reduce(item.Data, func(size int64, item BufferItem, _ int) int64 {
		return size + item.Size()
	}, int64(0))
}

type BufferItem struct {
	PartitionID int64
	DeleteData  storage.DeleteData
}

func (item *BufferItem) Size() int64 {
	var pkSize int64
	if len(item.DeleteData.Pks) > 0 {
		pkSize = int64(len(item.DeleteData.Pks)) * item.DeleteData.Pks[0].Size()
	}

	return int64(96) + pkSize + int64(8*len(item.DeleteData.Tss))
}
