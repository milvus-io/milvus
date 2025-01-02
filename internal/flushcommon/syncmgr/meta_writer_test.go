package syncmgr

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	err := s.writer.UpdateSync(ctx, task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	err := s.writer.UpdateSync(ctx, task)
	s.Error(err)
}

func TestMetaWriter(t *testing.T) {
	suite.Run(t, new(MetaWriterSuite))
}
