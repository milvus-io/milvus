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

type firstTimeTickWALImpls struct {
	channel     types.PChannelInfo
	appendCount atomic.Int32
	appendFunc  func(context.Context, message.MutableMessage) (message.MessageID, error)
}

func newFirstTimeTickWALImpls(appendFunc func(context.Context, message.MutableMessage) (message.MessageID, error)) *firstTimeTickWALImpls {
	return &firstTimeTickWALImpls{
		channel: types.PChannelInfo{
			Name:       "first-time-tick-test",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		appendFunc: appendFunc,
	}
}

func (w *firstTimeTickWALImpls) WALName() message.WALName {
	return message.WALNameTest
}

func (w *firstTimeTickWALImpls) Channel() types.PChannelInfo {
	return w.channel
}

func (w *firstTimeTickWALImpls) Close() {
}

func (w *firstTimeTickWALImpls) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	w.appendCount.Add(1)
	return w.appendFunc(ctx, msg)
}

func (w *firstTimeTickWALImpls) Read(context.Context, walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	panic("unexpected read")
}

func (w *firstTimeTickWALImpls) Truncate(context.Context, message.MessageID) error {
	panic("unexpected truncate")
}

func TestSendFirstTimeTickStopsOnFencedWAL(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, errors.Mark(errors.New("writer fenced"), walimpls.ErrFenced)
	})

	msg, err := sendFirstTimeTick(context.Background(), walImpls, nil)

	require.Error(t, err)
	assert.Nil(t, msg)
	assert.True(t, errors.Is(err, walimpls.ErrFenced))
	assert.Equal(t, int32(1), walImpls.appendCount.Load())
}

func TestSendFirstTimeTickFastFailsOnAppendError(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, errors.New("temporary wal failure")
	})

	msg, err := sendFirstTimeTick(context.Background(), walImpls, nil)

	require.Error(t, err)
	assert.Nil(t, msg)
	assert.Contains(t, err.Error(), "send first timestamp message failed")
	assert.Equal(t, int32(1), walImpls.appendCount.Load())
}

func TestSendFirstTimeTickFastFailsOnTSOAllocateError(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	mockAllocate := mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "Allocate")).
		Return(uint64(0), errors.New("allocate tso failed")).
		Build()
	defer mockAllocate.UnPatch()

	msg, err := sendFirstTimeTick(context.Background(), walImpls, nil)

	require.Error(t, err)
	assert.Nil(t, msg)
	assert.Contains(t, err.Error(), "allocate timestamp failed")
	assert.Equal(t, int32(0), walImpls.appendCount.Load())
}

func TestSendFirstTimeTickReturnsMessageOnSuccess(t *testing.T) {
	resource.InitForTest(t)
	walImpls := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})

	msg, err := sendFirstTimeTick(context.Background(), walImpls, nil)

	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.True(t, msg.MessageID().EQ(rmq.NewRmqID(1)))
	assert.Equal(t, int32(1), walImpls.appendCount.Load())
}
