package shards

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestPartitionManager(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel",
		Term: 1,
	}
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Return().Maybe()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	m1 := newTestSegmentAllocManager(channel, &messagespb.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1003,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 120)
	m2 := newTestSegmentAllocManager(channel, &messagespb.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      1004,
		StorageVersion: 2,
		MaxSegmentSize: 150,
	}, 130)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)
	m := newPartitionSegmentManager(ctx,
		log.With(),
		f,
		types.PChannelInfo{
			Name: "pchannel",
			Term: 1,
		},
		"v1",
		1,
		2,
		map[int64]*segmentAllocManager{
			m1.GetSegmentID(): m1,
			m2.GetSegmentID(): m2,
		},
		&mockedTxnManager{},
		100,
		metricsutil.NewSegmentAssignMetrics(channel.Name),
	)
	createSegmentDone := make(chan struct{}, 1)
	flushSegmentDone := make(chan struct{}, 1)
	msgTimeTick := uint64(200)
	w.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
			if rand.Int31n(2) == 0 {
				return nil, errors.New("random error")
			}

			switch msg.MessageType() {
			case message.MessageTypeCreateSegment:
				msg2 := msg.WithTimeTick(msgTimeTick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(4))
				m2 := message.MustAsImmutableCreateSegmentMessageV2(msg2)
				m.AddSegment(newSegmentAllocManager(channel, m2))
				createSegmentDone <- struct{}{}
			case message.MessageTypeFlush:
				msg2 := msg.WithTimeTick(msgTimeTick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(5))
				m2 := message.MustAsImmutableFlushMessageV2(msg2)
				m.MustRemoveFlushedSegment(m2.Header().SegmentId)
				flushSegmentDone <- struct{}{}
			}
			return &types.AppendResult{
				MessageID: rmq.NewRmqID(20),
				TimeTick:  200,
			}, nil
		}).Maybe()

	assert.NotNil(t, m.GetSegmentManager(m2.GetSegmentID()))
	assert.NotNil(t, m.GetSegmentManager(m1.GetSegmentID()))
	assert.Nil(t, m.GetSegmentManager(1))

	// There's no waiting growing segment
	<-m.WaitPendingGrowingSegmentReady()

	result, err := m.AssignSegment(&AssignSegmentRequest{
		TimeTick: 125,
		InsertMetrics: stats.InsertMetrics{
			Rows:       100,
			BinarySize: 120,
		},
	})
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFencedAssign)

	result, err = m.AssignSegment(&AssignSegmentRequest{
		TimeTick: 135,
		InsertMetrics: stats.InsertMetrics{
			Rows:       100,
			BinarySize: 120,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	result.Ack()
	req := &AssignSegmentRequest{
		TimeTick: 136,
		InsertMetrics: stats.InsertMetrics{
			Rows:       100,
			BinarySize: 120,
		},
	}

	result, _ = m.AssignSegment(req)
	result.Ack()
	_, err = m.AssignSegment(req)
	assert.ErrorIs(t, err, ErrWaitForNewSegment)

	<-createSegmentDone
	// should ready
	<-m.WaitPendingGrowingSegmentReady()

	_, err = m.AssignSegment(req)
	assert.ErrorIs(t, err, ErrTimeTickTooOld)
	req.TimeTick = 210
	result, err = m.AssignSegment(req)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	result.Ack()

	m.AsyncFlushSegment(utils.SealSegmentSignal{
		SegmentBelongs: utils.SegmentBelongs{
			SegmentID: m1.GetSegmentID(),
		},
		SealPolicy: policy.PolicyCapacity(),
	})
	<-flushSegmentDone

	assert.Nil(t, m.GetSegmentManager(m1.GetSegmentID()))

	segmentIDs := m.FlushAndFenceSegmentUntil(250)
	assert.Len(t, segmentIDs, 2)

	_, err = m.AssignSegment(req)
	assert.ErrorIs(t, err, ErrFencedAssign)

	req.TimeTick = 260
	msgTimeTick = 300
	_, err = m.AssignSegment(req)
	assert.ErrorIs(t, err, ErrWaitForNewSegment)

	<-createSegmentDone
	segmentIDs = m.FlushAndDropPartition(policy.PolicyPartitionRemoved())
	assert.Len(t, segmentIDs, 1)
	<-m.WaitPendingGrowingSegmentReady()
}

type mockedTxnManager struct{}

func (m *mockedTxnManager) RecoverDone() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
