package writebuffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type SyncPolicySuite struct {
	suite.Suite
	collSchema *schemapb.CollectionSchema
}

func (s *SyncPolicySuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())

	s.collSchema = &schemapb.CollectionSchema{
		Name: "wb_base_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
}

func (s *SyncPolicySuite) TestSyncFullBuffer() {
	buffer, err := newSegmentBuffer(100, s.collSchema)
	s.Require().NoError(err)

	policy := GetFullBufferPolicy()
	ids := policy.SelectSegments([]*segmentBuffer{buffer}, 0)
	s.Equal(0, len(ids), "empty buffer shall not be synced")

	buffer.insertBuffer.size = buffer.insertBuffer.sizeLimit + 1

	ids = policy.SelectSegments([]*segmentBuffer{buffer}, 0)
	s.ElementsMatch([]int64{100}, ids)
}

func (s *SyncPolicySuite) TestSyncStalePolicy() {
	policy := GetSyncStaleBufferPolicy(2 * time.Minute)

	buffer, err := newSegmentBuffer(100, s.collSchema)
	s.Require().NoError(err)

	ids := policy.SelectSegments([]*segmentBuffer{buffer}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.Equal(0, len(ids), "empty buffer shall not be synced")

	buffer.insertBuffer.startPos = &msgpb.MsgPosition{
		Timestamp: tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute*3), 0),
	}

	ids = policy.SelectSegments([]*segmentBuffer{buffer}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch([]int64{100}, ids)

	buffer.insertBuffer.startPos = &msgpb.MsgPosition{
		Timestamp: tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute), 0),
	}

	ids = policy.SelectSegments([]*segmentBuffer{buffer}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.Equal(0, len(ids), "")
}

func (s *SyncPolicySuite) TestSyncDroppedPolicy() {
	metacache := metacache.NewMockMetaCache(s.T())
	policy := GetDroppedSegmentPolicy(metacache)
	ids := []int64{1, 2, 3}
	metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return(ids)
	result := policy.SelectSegments([]*segmentBuffer{}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch(ids, result)
}

func (s *SyncPolicySuite) TestSealedSegmentsPolicy() {
	metacache := metacache.NewMockMetaCache(s.T())
	policy := GetSealedSegmentsPolicy(metacache)
	ids := []int64{1, 2, 3}
	metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return(ids)
	metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()

	result := policy.SelectSegments([]*segmentBuffer{}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch(ids, result)
}

func (s *SyncPolicySuite) TestOlderBufferPolicy() {
	policy := GetOldestBufferPolicy(2)

	type testCase struct {
		tag     string
		buffers []*segmentBuffer
		expect  []int64
	}

	cases := []*testCase{
		{tag: "empty_buffers", buffers: nil, expect: []int64{}},
		{tag: "3_candidates", buffers: []*segmentBuffer{
			{
				segmentID:    100,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{startPos: &msgpb.MsgPosition{Timestamp: 1}}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{}},
			},
			{
				segmentID:    200,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{startPos: &msgpb.MsgPosition{Timestamp: 2}}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{}},
			},
			{
				segmentID:    300,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{startPos: &msgpb.MsgPosition{Timestamp: 3}}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{}},
			},
		}, expect: []int64{100, 200}},
		{tag: "1_candidates", buffers: []*segmentBuffer{
			{
				segmentID:    100,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{startPos: &msgpb.MsgPosition{Timestamp: 1}}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{}},
			},
		}, expect: []int64{100}},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.ElementsMatch(tc.expect, policy.SelectSegments(tc.buffers, 0))
		})
	}
}

func TestSyncPolicy(t *testing.T) {
	suite.Run(t, new(SyncPolicySuite))
}
