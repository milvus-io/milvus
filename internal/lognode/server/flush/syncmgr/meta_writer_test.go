package syncmgr

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/flush/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type MetaWriterSuite struct {
	suite.Suite

	broker *broker.MockBroker
	meta   *meta.MockMetaCache

	writer MetaWriter
}

func (s *MetaWriterSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *MetaWriterSuite) SetupTest() {
	s.broker = broker.NewMockBroker(s.T())
	s.meta = meta.NewMockMetaCache(s.T())
	s.writer = BrokerMetaWriter(s.broker, 1, retry.Attempts(1))
}

func (s *MetaWriterSuite) TestNormalSave() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	bfs := meta.NewBloomFilterSet()
	seg := meta.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	meta.UpdateNumOfRows(1000)(seg)
	s.meta.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*meta.SegmentInfo{seg})
	s.meta.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.meta.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
	task := NewSyncTask()
	task.WithMetaCache(s.meta)
	err := s.writer.UpdateSync(task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnError() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := meta.NewBloomFilterSet()
	seg := meta.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	meta.UpdateNumOfRows(1000)(seg)
	s.meta.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.meta.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*meta.SegmentInfo{seg})
	task := NewSyncTask()
	task.WithMetaCache(s.meta)
	err := s.writer.UpdateSync(task)
	s.Error(err)
}

func (s *MetaWriterSuite) TestNormalSaveV2() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	bfs := meta.NewBloomFilterSet()
	seg := meta.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	meta.UpdateNumOfRows(1000)(seg)
	s.meta.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.meta.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*meta.SegmentInfo{seg})
	task := NewSyncTaskV2()
	task.WithMetaCache(s.meta)
	err := s.writer.UpdateSyncV2(task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnErrorV2() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := meta.NewBloomFilterSet()
	seg := meta.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	meta.UpdateNumOfRows(1000)(seg)
	s.meta.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.meta.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*meta.SegmentInfo{seg})
	task := NewSyncTaskV2()
	task.WithMetaCache(s.meta)
	err := s.writer.UpdateSyncV2(task)
	s.Error(err)
}

func TestMetaWriter(t *testing.T) {
	suite.Run(t, new(MetaWriterSuite))
}
