package adaptor

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

type recoveryBarrierWALImpls struct {
	channel     types.PChannelInfo
	appendCount atomic.Int32
	appendFunc  func(context.Context, message.MutableMessage) (message.MessageID, error)
}

func newRecoveryBarrierWALImpls(appendFunc func(context.Context, message.MutableMessage) (message.MessageID, error)) *recoveryBarrierWALImpls {
	return &recoveryBarrierWALImpls{
		channel: types.PChannelInfo{
			Name:       "recovery-barrier-test",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		appendFunc: appendFunc,
	}
}

func newFirstTimeTickWALImpls(appendFunc func(context.Context, message.MutableMessage) (message.MessageID, error)) *recoveryBarrierWALImpls {
	return newRecoveryBarrierWALImpls(appendFunc)
}

func (w *recoveryBarrierWALImpls) WALName() message.WALName {
	return message.WALNameTest
}

func (w *recoveryBarrierWALImpls) Channel() types.PChannelInfo {
	return w.channel
}

func (w *recoveryBarrierWALImpls) Close() {
}

func (w *recoveryBarrierWALImpls) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	w.appendCount.Add(1)
	return w.appendFunc(ctx, msg)
}

func (w *recoveryBarrierWALImpls) Read(context.Context, walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	panic("unexpected read")
}

func (w *recoveryBarrierWALImpls) Truncate(context.Context, message.MessageID) error {
	panic("unexpected truncate")
}

func TestSendRecoveryBarrierStopsOnFencedWAL(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newRecoveryBarrierWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, errors.Mark(errors.New("writer fenced"), walimpls.ErrFenced)
	})

	msg, err := sendRecoveryBarrier(context.Background(), walImpls, nil)

	require.Error(t, err)
	assert.Nil(t, msg)
	assert.True(t, errors.Is(err, walimpls.ErrFenced))
	assert.Equal(t, int32(1), walImpls.appendCount.Load())
}

func TestSendRecoveryBarrierFastFailsOnAppendError(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newRecoveryBarrierWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, errors.New("temporary wal failure")
	})

	msg, err := sendRecoveryBarrier(context.Background(), walImpls, nil)

	require.Error(t, err)
	assert.Nil(t, msg)
	assert.Contains(t, err.Error(), "append recovery barrier message failed")
	assert.Equal(t, int32(1), walImpls.appendCount.Load())
}

func TestSendRecoveryBarrierFastFailsOnTSOAllocateError(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newRecoveryBarrierWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	mockAllocate := mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "Allocate")).
		Return(uint64(0), errors.New("allocate tso failed")).
		Build()
	defer mockAllocate.UnPatch()

	msg, err := sendRecoveryBarrier(context.Background(), walImpls, nil)

	require.Error(t, err)
	assert.Nil(t, msg)
	assert.Contains(t, err.Error(), "allocate timestamp failed")
	assert.Equal(t, int32(0), walImpls.appendCount.Load())
}

func TestSendRecoveryBarrierReturnsMessageOnSuccess(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newRecoveryBarrierWALImpls(func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		assert.Equal(t, message.MessageTypeRecoveryBarrier, msg.MessageType())
		assert.True(t, msg.IsPersisted())
		assert.Empty(t, msg.VChannel())
		return rmq.NewRmqID(1), nil
	})

	msg, err := sendRecoveryBarrier(context.Background(), walImpls, nil)

	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.True(t, msg.MessageID().EQ(rmq.NewRmqID(1)))
	assert.Equal(t, int32(1), walImpls.appendCount.Load())
}
