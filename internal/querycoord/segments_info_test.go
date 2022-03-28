package querycoord

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

func Test_segmentsInfo_getSegment(t *testing.T) {
	s := newSegmentsInfo(createTestKv(t))
	assert.Nil(t, s.loadSegments())
	got := s.getSegment(1)
	assert.EqualValues(t, 1, got.GetSegmentID())
	got = s.getSegment(2)
	assert.EqualValues(t, 2, got.GetSegmentID())

	segment := &querypb.SegmentInfo{SegmentID: 3, CollectionID: 3}
	assert.Nil(t, s.saveSegment(segment))
	got = s.getSegment(3)
	assert.NotNil(t, got)
	assert.True(t, proto.Equal(segment, got))

	assert.Nil(t, s.removeSegment(segment))
	got = s.getSegment(3)
	assert.Nil(t, got)
}

func Test_segmentsInfo_getSegments(t *testing.T) {
	s := newSegmentsInfo(createTestKv(t))
	assert.Nil(t, s.loadSegments())
	got := s.getSegments()
	assert.ElementsMatch(t, []int64{1, 2}, collectSegmentIDs(got))

	segment := &querypb.SegmentInfo{SegmentID: 3, CollectionID: 3}
	assert.Nil(t, s.saveSegment(segment))
	got = s.getSegments()
	assert.ElementsMatch(t, []int64{1, 2, 3}, collectSegmentIDs(got))
	assert.Nil(t, s.saveSegment(segment))
	got = s.getSegments()
	assert.ElementsMatch(t, []int64{1, 2, 3}, collectSegmentIDs(got))

	assert.Nil(t, s.removeSegment(segment))
	got = s.getSegments()
	assert.ElementsMatch(t, []int64{1, 2}, collectSegmentIDs(got))
}

func createTestKv(t *testing.T) kv.TxnKV {
	kv := memkv.NewMemoryKV()
	segments := []*querypb.SegmentInfo{
		{SegmentID: 1, CollectionID: 1},
		{SegmentID: 2, CollectionID: 2},
	}
	for _, segment := range segments {
		k := getSegmentKey(segment)
		v, err := proto.Marshal(segment)
		assert.Nil(t, err)
		assert.Nil(t, kv.Save(k, string(v)))
	}
	return kv
}

func collectSegmentIDs(segments []*querypb.SegmentInfo) []int64 {
	res := make([]int64, 0, len(segments))
	for _, s := range segments {
		res = append(res, s.GetSegmentID())
	}
	return res
}
