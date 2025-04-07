package flowcontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

func TestFlowcontrolManager(t *testing.T) {
	ctx := context.Background()

	fm := newFlowcontrolManager()

	// not data message always pass the request bytes.
	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().MessageType().Unset()
	msg.EXPECT().MessageType().Return(message.MessageTypeTimeTick)
	err := fm.RequestBytes(ctx, msg)
	assert.NoError(t, err)

	// data message with size less than 0 always pass the request bytes.
	msg.EXPECT().MessageType().Unset()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert)
	msg.EXPECT().EstimateSize().Return(20 * 1024 * 1024)
	err = fm.RequestBytes(ctx, msg)
	assert.NoError(t, err)

	// convert into deny state
	fm.stateChange(hardware.SystemMetrics{UsedMemoryBytes: 98, TotalMemoryBytes: 100})
	err = fm.RequestBytes(ctx, msg)
	assert.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsFlowControlDenied())

	// because the state is deny, even the memory is less than 90%, the state is still dennied.
	fm.stateChange(hardware.SystemMetrics{UsedMemoryBytes: 90, TotalMemoryBytes: 100})
	err = fm.RequestBytes(ctx, msg)
	assert.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsFlowControlDenied())

	// the memory is less than 80%, the state is normal.
	fm.stateChange(hardware.SystemMetrics{UsedMemoryBytes: 79, TotalMemoryBytes: 100})
	err = fm.RequestBytes(ctx, msg)
	assert.NoError(t, err)

	// the memory is greater than 85, the state is throttling.
	fm.stateChange(hardware.SystemMetrics{UsedMemoryBytes: 86, TotalMemoryBytes: 100})
	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()

		err := fm.RequestBytes(ctx, msg)
		return errors.Is(err, context.DeadlineExceeded)
	}, 1*time.Second, 1*time.Millisecond)

	fm.stateChange(hardware.SystemMetrics{UsedMemoryBytes: 81, TotalMemoryBytes: 100})
	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()

		err := fm.RequestBytes(ctx, msg)
		return errors.Is(err, context.DeadlineExceeded)
	}, 1*time.Second, 1*time.Millisecond)

	// the memory is less than 80%, the state is normal.
	fm.stateChange(hardware.SystemMetrics{UsedMemoryBytes: 79, TotalMemoryBytes: 100})
	err = fm.RequestBytes(ctx, msg)
	assert.NoError(t, err)
}
