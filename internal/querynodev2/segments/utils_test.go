package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestFilterZeroValuesFromSlice(t *testing.T) {
	var ints []int64
	ints = append(ints, 10)
	ints = append(ints, 0)
	ints = append(ints, 5)
	ints = append(ints, 13)
	ints = append(ints, 0)

	filteredInts := FilterZeroValuesFromSlice(ints)
	assert.Equal(t, 3, len(filteredInts))
	assert.EqualValues(t, []int64{10, 5, 13}, filteredInts)
}

func TestGetSegmentRelatedDataSize(t *testing.T) {
	t.Run("seal segment", func(t *testing.T) {
		segment := NewMockSegment(t)
		segment.EXPECT().Type().Return(SegmentTypeSealed)
		segment.EXPECT().LoadInfo().Return(&querypb.SegmentLoadInfo{
			BinlogPaths: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 10,
						},
						{
							LogSize: 20,
						},
					},
				},
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 30,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 30,
						},
					},
				},
			},
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 10,
						},
					},
				},
			},
		})
		assert.EqualValues(t, 100, GetSegmentRelatedDataSize(segment))
	})

	t.Run("growing segment", func(t *testing.T) {
		segment := NewMockSegment(t)
		segment.EXPECT().Type().Return(SegmentTypeGrowing)
		segment.EXPECT().MemSize().Return(int64(100))
		assert.EqualValues(t, 100, GetSegmentRelatedDataSize(segment))
	})
}
