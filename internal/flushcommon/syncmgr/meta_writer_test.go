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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
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

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
		Statslogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
		Bm25Statslogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "test"}},
			},
		},
	}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
			s.Equal(1, len(req.Field2BinlogPaths))
			s.Equal(1, len(req.Field2Bm25LogPaths))
			s.Equal(1, len(req.Field2StatslogPaths))
			s.Equal(1, len(req.Deltalogs))
			return nil
		})

	err := s.writer.UpdateSync(ctx, task)
	s.NoError(err)
}

func (s *MetaWriterSuite) TestReturnError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	err := s.writer.UpdateSync(ctx, task)
	s.Error(err)
}

// DataCoord returns ErrChannelNotFound from its ownership check, and its own
// comment there says the rejection can happen while the flusher is ready but
// the coordinator has not yet observed the assignment, so the caller should
// retry. Reporting success would acknowledge rows DataCoord never recorded and
// let the channel checkpoint advance past them.
func (s *MetaWriterSuite) TestChannelNotFoundRetries() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.writer = BrokerMetaWriter(s.broker, 1, retry.Attempts(5))

	calls := 0
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *datapb.SaveBinlogPathsRequest) error {
			calls++
			if calls < 3 {
				return merr.WrapErrChannelNotFound("ch-1")
			}
			return nil
		})

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(new(SyncPack))
	err := s.writer.UpdateSync(ctx, task)

	s.NoError(err, "the retry eventually succeeds")
	s.Equal(3, calls, "channel-not-found must be retried, not swallowed as success")
}

// A segment id is never reissued, so a segment DataCoord no longer knows cannot
// come back and no metadata is left for these rows. Terminal, and surfaced so
// the caller settles the reservation as discarded rather than as committed.
func (s *MetaWriterSuite) TestSegmentNotFoundIsTerminal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.writer = BrokerMetaWriter(s.broker, 1, retry.Attempts(5))

	calls := 0
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *datapb.SaveBinlogPathsRequest) error {
			calls++
			return merr.WrapErrSegmentNotFound(int64(1))
		})

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})

	pack := new(SyncPack)
	pack.WithFlush()
	task := NewSyncTask().WithMetaCache(s.metacache).WithSyncPack(pack)
	err := s.writer.UpdateSync(ctx, task)

	s.ErrorIs(err, merr.ErrSegmentNotFound, "surfaced so the caller can discard")
	s.Equal(1, calls, "terminal, so exactly one attempt")
}

func TestMetaWriter(t *testing.T) {
	suite.Run(t, new(MetaWriterSuite))
}
