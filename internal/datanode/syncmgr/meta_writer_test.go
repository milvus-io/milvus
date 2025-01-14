package syncmgr

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type MetaWriterSuite struct {
	suite.Suite

	broker    *broker.MockBroker
	metacache *metacache.MockMetaCache

	writer MetaWriter
}

func (s *MetaWriterSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *MetaWriterSuite) SetupTest() {
	s.broker = broker.NewMockBroker(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.writer = BrokerMetaWriter(s.broker, 1, retry.Attempts(1))
}

func (s *MetaWriterSuite) TestNormalSave() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	bfs := metacache.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
	task := NewSyncTask()
	task.WithMetaCache(s.metacache)
	err := s.writer.UpdateSync(task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnError() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := metacache.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything).Return([]*metacache.SegmentInfo{seg})
	task := NewSyncTask()
	task.WithMetaCache(s.metacache)
	err := s.writer.UpdateSync(task)
	s.Error(err)
}

func (s *MetaWriterSuite) TestNormalSaveV2() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	bfs := metacache.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything).Return([]*metacache.SegmentInfo{seg})
	task := NewSyncTaskV2()
	task.WithMetaCache(s.metacache)
	err := s.writer.UpdateSyncV2(task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnErrorV2() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := metacache.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything).Return([]*metacache.SegmentInfo{seg})
	task := NewSyncTaskV2()
	task.WithMetaCache(s.metacache)
	err := s.writer.UpdateSyncV2(task)
	s.Error(err)
}

func TestMetaWriter(t *testing.T) {
	suite.Run(t, new(MetaWriterSuite))
}
