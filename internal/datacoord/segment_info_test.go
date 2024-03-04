package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestCompactionTo(t *testing.T) {
	segments := NewSegmentsInfo()
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1,
	})
	segments.SetSegment(segment.GetID(), segment)

	s, ok := segments.GetCompactionTo(1)
	assert.True(t, ok)
	assert.Nil(t, s)

	segment = NewSegmentInfo(&datapb.SegmentInfo{
		ID: 2,
	})
	segments.SetSegment(segment.GetID(), segment)
	segment = NewSegmentInfo(&datapb.SegmentInfo{
		ID:             3,
		CompactionFrom: []int64{1, 2},
	})
	segments.SetSegment(segment.GetID(), segment)

	s, ok = segments.GetCompactionTo(3)
	assert.Nil(t, s)
	assert.True(t, ok)
	s, ok = segments.GetCompactionTo(1)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, int64(3), s.GetID())
	s, ok = segments.GetCompactionTo(2)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, int64(3), s.GetID())

	// should be overwrite.
	segment = NewSegmentInfo(&datapb.SegmentInfo{
		ID:             3,
		CompactionFrom: []int64{2},
	})
	segments.SetSegment(segment.GetID(), segment)

	s, ok = segments.GetCompactionTo(3)
	assert.True(t, ok)
	assert.Nil(t, s)
	s, ok = segments.GetCompactionTo(1)
	assert.True(t, ok)
	assert.Nil(t, s)
	s, ok = segments.GetCompactionTo(2)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, int64(3), s.GetID())

	// should be overwrite back.
	segment = NewSegmentInfo(&datapb.SegmentInfo{
		ID:             3,
		CompactionFrom: []int64{1, 2},
	})
	segments.SetSegment(segment.GetID(), segment)

	s, ok = segments.GetCompactionTo(3)
	assert.Nil(t, s)
	assert.True(t, ok)
	s, ok = segments.GetCompactionTo(1)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, int64(3), s.GetID())
	s, ok = segments.GetCompactionTo(2)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, int64(3), s.GetID())

	// should be droped.
	segments.DropSegment(1)
	s, ok = segments.GetCompactionTo(1)
	assert.False(t, ok)
	assert.Nil(t, s)
	s, ok = segments.GetCompactionTo(2)
	assert.True(t, ok)
	assert.NotNil(t, s)
	assert.Equal(t, int64(3), s.GetID())
	s, ok = segments.GetCompactionTo(3)
	assert.Nil(t, s)
	assert.True(t, ok)

	segments.DropSegment(3)
	s, ok = segments.GetCompactionTo(2)
	assert.True(t, ok)
	assert.Nil(t, s)
}
