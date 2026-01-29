package writebuffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
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

	fullBuffers := make(map[int64]struct{})
	policy := GetFullBufferPolicyWithTracker(func() []int64 {
		result := make([]int64, 0, len(fullBuffers))
		for id := range fullBuffers {
			result = append(result, id)
		}
		return result
	})
	ids := policy.SelectSegments(0)
	s.Equal(0, len(ids), "empty buffer shall not be synced")

	buffer.insertBuffer.size = buffer.insertBuffer.sizeLimit + 1
	fullBuffers[100] = struct{}{}

	ids = policy.SelectSegments(0)
	s.ElementsMatch([]int64{100}, ids)
}

func (s *SyncPolicySuite) TestSyncStalePolicy() {
	bufferHeap := NewBufferTimestampHeap()
	policy := GetSyncStaleBufferPolicyWithHeap(2*time.Minute, bufferHeap)

	buffer, err := newSegmentBuffer(100, s.collSchema)
	s.Require().NoError(err)

	ids := policy.SelectSegments(tsoutil.ComposeTSByTime(time.Now(), 0))
	s.Equal(0, len(ids), "empty buffer shall not be synced")

	buffer.insertBuffer.startPos = &msgpb.MsgPosition{
		Timestamp: tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute*3), 0),
	}
	bufferHeap.Update(100, buffer.MinTimestamp())

	ids = policy.SelectSegments(tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch([]int64{100}, ids)

	buffer.insertBuffer.startPos = &msgpb.MsgPosition{
		Timestamp: tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute), 0),
	}
	// Remove and re-add to update timestamp
	bufferHeap.Remove(100)
	bufferHeap.Update(100, buffer.MinTimestamp())

	ids = policy.SelectSegments(tsoutil.ComposeTSByTime(time.Now(), 0))
	s.Equal(0, len(ids), "")
}

func (s *SyncPolicySuite) TestSyncDroppedPolicy() {
	metacache := metacache.NewMockMetaCache(s.T())
	policy := GetDroppedSegmentPolicy(metacache)
	ids := []int64{1, 2, 3}
	metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return(ids)
	result := policy.SelectSegments(tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch(ids, result)
}

func (s *SyncPolicySuite) TestSealedSegmentsPolicy() {
	metacache := metacache.NewMockMetaCache(s.T())
	policy := GetSealedSegmentsPolicy(metacache)
	ids := []int64{1, 2, 3}
	metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return(ids)
	metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()

	result := policy.SelectSegments(tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch(ids, result)
}

func (s *SyncPolicySuite) TestOlderBufferPolicy() {
	bufferHeap := NewBufferTimestampHeap()
	policy := GetOldestBufferPolicyWithHeap(2, bufferHeap)

	type testCase struct {
		tag    string
		setup  func()
		expect []int64
	}

	cases := []*testCase{
		{tag: "empty_buffers", setup: func() {}, expect: nil},
		{tag: "3_candidates", setup: func() {
			bufferHeap.Update(100, 1)
			bufferHeap.Update(200, 2)
			bufferHeap.Update(300, 3)
		}, expect: []int64{100, 200}},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			// Reset heap for each test
			bufferHeap = NewBufferTimestampHeap()
			policy = GetOldestBufferPolicyWithHeap(2, bufferHeap)
			tc.setup()
			s.ElementsMatch(tc.expect, policy.SelectSegments(0))
		})
	}
}

func TestSyncPolicy(t *testing.T) {
	suite.Run(t, new(SyncPolicySuite))
}
