package writebuffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
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

func (s *SyncPolicySuite) TestSealedSegmentsPolicy() {
	metacache := metacache.NewMockMetaCache(s.T())
	policy := GetSealedSegmentsPolicy(metacache)
	ids := []int64{1, 2, 3}
	metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return(ids)
	metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()

	result := policy.SelectSegments([]*segmentBuffer{}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch(ids, result)
}

func (s *SyncPolicySuite) TestCompactedSegmentsPolicy() {
	metacache := metacache.NewMockMetaCache(s.T())
	policy := GetCompactedSegmentsPolicy(metacache)
	ids := []int64{1, 2}
	metacache.EXPECT().GetSegmentIDsBy(mock.Anything, mock.Anything).Return(ids)

	result := policy.SelectSegments([]*segmentBuffer{{segmentID: 1}, {segmentID: 2}}, tsoutil.ComposeTSByTime(time.Now(), 0))
	s.ElementsMatch(ids, result)
}

func (s *SyncPolicySuite) TestMemoryHighPolicy() {
	memoryHigh := atomic.NewBool(false)
	policy := GetMemoryHighPolicy(memoryHigh)

	s.Run("memory_high_false", func() {
		memoryHigh.Store(false)
		buffers := []*segmentBuffer{
			{
				segmentID:    100,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{size: 100}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{size: 100}},
			},
		}

		result := policy.SelectSegments(buffers, 1000)

		s.Len(result, 0)
	})

	s.Run("memory_high_true", func() {
		memoryHigh.Store(true)
		param := paramtable.Get()

		param.Save(param.DataNodeCfg.MemoryForceSyncMinSize.Key, "200")
		param.Save(param.DataNodeCfg.MemoryForceSyncSegmentNum.Key, "2")
		defer param.Reset(param.DataNodeCfg.MemoryForceSyncMinSize.Key)
		defer param.Reset(param.DataNodeCfg.MemoryForceSyncSegmentNum.Key)

		buffers := []*segmentBuffer{
			{
				segmentID:    100,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{size: 100}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{size: 99}},
			},
			{
				segmentID:    200,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{size: 200}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{size: 200}},
			},
			{
				segmentID:    300,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{size: 300}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{size: 300}},
			},
			{
				segmentID:    400,
				insertBuffer: &InsertBuffer{BufferBase: BufferBase{size: 400}},
				deltaBuffer:  &DeltaBuffer{BufferBase: BufferBase{size: 400}},
			},
		}

		result := policy.SelectSegments(buffers, 1000)

		s.Len(result, 2)
		s.ElementsMatch([]int64{300, 400}, result)
	})
}

func TestSyncPolicy(t *testing.T) {
	suite.Run(t, new(SyncPolicySuite))
}
