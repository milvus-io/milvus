//go:build test && dynamic

package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
)

func TestViewQueryGrowingSegmentBatchPkExistIsConservative(t *testing.T) {
	segment := NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{CollectionID: 10}, nil)

	hits := segment.BatchPkExist(storage.NewBatchLocationsCache([]storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(2),
	}))

	assert.Equal(t, []bool{true, true}, hits)
}
