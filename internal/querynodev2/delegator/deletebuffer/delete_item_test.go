package deletebuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
)

func TestDeleteBufferItem(t *testing.T) {
	item := &BufferItem{
		PartitionID: 100,
		DeleteData:  storage.DeleteData{},
	}

	assert.Equal(t, int64(96), item.Size())
	assert.EqualValues(t, 0, item.EntryNum())

	item.DeleteData.Pks = []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(10),
	}
	item.DeleteData.Tss = []uint64{2000}
	assert.Equal(t, int64(120), item.Size())
	assert.EqualValues(t, 1, item.EntryNum())
}
