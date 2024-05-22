package timestamp

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestTimestampAllocator(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := newMockRootCoordClient(t)
	allocator := NewAllocator(client)

	for i := 0; i < 5000; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
	}

	for i := 0; i < 100; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
		time.Sleep(time.Millisecond * 1)
		allocator.Sync()
	}
}
