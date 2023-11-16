package writebuffer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type WriteBufferSuite struct {
	suite.Suite
	collID      int64
	channelName string
	collSchema  *schemapb.CollectionSchema
	wb          *writeBufferBase
	syncMgr     *syncmgr.MockSyncManager
	metacache   *metacache.MockMetaCache
}

func (s *WriteBufferSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collID = 100
	s.collSchema = &schemapb.CollectionSchema{
		Name: "wb_base_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
	s.channelName = "by-dev-rootcoord-dml_0v0"
}

func (s *WriteBufferSuite) SetupTest() {
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.metacache.EXPECT().Schema().Return(s.collSchema).Maybe()
	s.metacache.EXPECT().Collection().Return(s.collID).Maybe()
	s.wb = newWriteBufferBase(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		pkStatsFactory: func(vchannel *datapb.SegmentInfo) *metacache.BloomFilterSet {
			return metacache.NewBloomFilterSet()
		},
	})
}

func (s *WriteBufferSuite) TestWriteBufferType() {
	wb, err := NewWriteBuffer(s.channelName, s.metacache, s.syncMgr, WithDeletePolicy(DeletePolicyBFPkOracle))
	s.NoError(err)

	_, ok := wb.(*bfWriteBuffer)
	s.True(ok)

	wb, err = NewWriteBuffer(s.channelName, s.metacache, s.syncMgr, WithDeletePolicy(DeletePolicyL0Delta), WithIDAllocator(allocator.NewMockGIDAllocator()))
	s.NoError(err)
	_, ok = wb.(*l0WriteBuffer)
	s.True(ok)

	_, err = NewWriteBuffer(s.channelName, s.metacache, s.syncMgr, WithDeletePolicy(""))
	s.Error(err)
}

func (s *WriteBufferSuite) TestHasSegment() {
	segmentID := int64(1001)

	s.False(s.wb.HasSegment(segmentID))

	s.wb.getOrCreateBuffer(segmentID)

	s.True(s.wb.HasSegment(segmentID))
}

func (s *WriteBufferSuite) TestFlushSegments() {
	segmentID := int64(1001)

	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything)

	wb, err := NewWriteBuffer(s.channelName, s.metacache, s.syncMgr, WithDeletePolicy(DeletePolicyBFPkOracle))
	s.NoError(err)

	err = wb.FlushSegments(context.Background(), []int64{segmentID})
	s.NoError(err)
}

func TestWriteBufferBase(t *testing.T) {
	suite.Run(t, new(WriteBufferSuite))
}
