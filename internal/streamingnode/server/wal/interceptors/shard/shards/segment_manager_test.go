package shards

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// newTestSegmentAllocManager creates a new segment allocation manager for testing.
func newTestSegmentAllocManager(
	channel types.PChannelInfo,
	h *message.CreateSegmentMessageHeader,
	timetick uint64,
) *segmentAllocManager {
	msg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(h).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(1))
	return newSegmentAllocManager(channel, message.MustAsImmutableCreateSegmentMessageV2(msg))
}

func TestSegmentAllocManager(t *testing.T) {
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

	m := newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		MaxSegmentSize: 100,
		StorageVersion: 2,
	}, 110)
	assert.NotNil(t, m)

	assert.Equal(t, m.CreateSegmentTimeTick(), uint64(110))
	assert.Equal(t, m.GetCollectionID(), int64(1))
	assert.Equal(t, m.GetPartitionID(), int64(2))
	assert.Equal(t, m.GetSegmentID(), int64(3))
	assert.Equal(t, m.GetVChannel(), "v1")
	assert.Equal(t, m.AckSem(), int32(0))
	assert.Equal(t, m.TxnSem(), int32(0))
	assert.False(t, m.IsFlushed())

	result, err := m.AllocRows(&AssignSegmentRequest{
		TimeTick: 110,
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTimeTickTooOld)
	assert.Nil(t, result)

	result, err = m.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       100,
			BinarySize: 30,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, result.SegmentID, int64(3))
	assert.NotNil(t, result.Acknowledge)
	assert.Equal(t, m.AckSem(), int32(1))
	assert.Equal(t, m.TxnSem(), int32(0))

	result, err = m.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       100,
			BinarySize: 50,
		},
		TxnSession: &mockedSession{},
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, m.TxnSem(), int32(1))
	assert.Equal(t, m.AckSem(), int32(2))

	result, err = m.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       100,
			BinarySize: 30,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, m.TxnSem(), int32(1))
	assert.Equal(t, m.AckSem(), int32(3))
	stat := resource.Resource().SegmentStatsManager().GetStatsOfSegment(3)
	assert.Equal(t, stats.ModifiedMetrics{Rows: 300, BinarySize: 110}, stat.Modified)
	assert.True(t, stat.ShouldBeSealed())

	result, err = m.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       1,
			BinarySize: 1,
		},
	})
	assert.ErrorIs(t, err, ErrNotEnoughSpace)
	assert.Nil(t, result)
	assert.Equal(t, m.TxnSem(), int32(1))
	assert.Equal(t, m.AckSem(), int32(3))

	assert.Panics(t, func() {
		m.GetFlushedStat()
	})
	assert.Panics(t, func() {
		m.SealPolicy()
	})

	m.Flush(policy.PolicyCapacity())
	m.Flush(policy.PolicyCapacity())
	assert.NotNil(t, m.SealPolicy())
	assert.NotNil(t, m.GetFlushedStat())

	assert.Panics(t, func() {
		m.GetStatFromRecovery()
	})

	_, err = m.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotGrowing)
}

func TestSegmentAllocManagerAcceptsOversizedFirstAllocation(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name: "test_channel_oversized",
		Term: 1,
	}

	sealObserved := make(chan int32, 1)
	var m *segmentAllocManager
	o := mock_utils.NewMockSealOperator(t)
	o.EXPECT().Channel().Return(channel)
	o.EXPECT().AsyncFlushSegment(mock.Anything).Run(func(utils.SealSegmentSignal) {
		sealObserved <- m.AckSem()
	}).Return().Once()
	resource.Resource().SegmentStatsManager().RegisterSealOperator(o, nil, nil)

	m = newTestSegmentAllocManager(channel, &message.CreateSegmentMessageHeader{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		MaxSegmentSize: 100,
		StorageVersion: 2,
	}, 110)

	result, err := m.AllocRows(&AssignSegmentRequest{
		TimeTick: 120,
		ModifiedMetrics: stats.ModifiedMetrics{
			Rows:       100,
			BinarySize: 120,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(3), result.SegmentID)
	assert.Equal(t, int32(1), m.AckSem())
	stat := resource.Resource().SegmentStatsManager().GetStatsOfSegment(3)
	assert.Equal(t, stats.ModifiedMetrics{Rows: 100, BinarySize: 120}, stat.Modified)
	assert.True(t, stat.ShouldBeSealed())

	select {
	case ackSem := <-sealObserved:
		assert.Equal(t, int32(1), ackSem, "capacity seal must wait for the oversized insert acknowledgement")
	case <-time.After(10 * time.Second):
		t.Fatal("capacity seal was not triggered")
	}

	result.Ack()
	assert.Zero(t, m.AckSem())
}

type mockedSession struct{}

func (m *mockedSession) RegisterCleanup(cleanup func(), timetick uint64) {
}
