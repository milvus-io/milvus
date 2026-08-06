package wab

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestWriteAheadBuffer(t *testing.T) {
	// Concurrent add message into bufffer and make syncup.
	// The reader should never lost any message if no eviction happen.
	wb := NewWriteAheadBuffer(newTestMaintenanceManager(t), "pchannel", mlog.With(), 5*1024*1024, 30*time.Second, createTimeTickMessage(1))
	t.Cleanup(wb.Close)
	expectedLastTimeTick := uint64(10000)
	ch := make(chan struct{})
	totalCnt := 0
	go func() {
		defer close(ch)
		nextTimeTick := uint64(100)
		for {
			msgs := make([]message.ImmutableMessage, 0)
			for i := 0; i < int(rand.Int31n(10))+1; i++ {
				nextTimeTick += uint64(rand.Int31n(100) + 1)
				msgs = append(msgs, createInsertMessage(nextTimeTick))
				if nextTimeTick > expectedLastTimeTick {
					break
				}
			}
			wb.Append(msgs, createTimeTickMessage(msgs[len(msgs)-1].TimeTick()))
			totalCnt += (len(msgs) + 1)
			if nextTimeTick > expectedLastTimeTick {
				break
			}
		}
	}()
	if rand.Int31n(2) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	r1, err := wb.ReadFromExclusiveTimeTick(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, r1)
	lastTimeTick := uint64(0)
	timeticks := make([]uint64, 0)
	for {
		msg, err := r1.Next(context.Background())
		assert.NoError(t, err)
		if msg.MessageType() == message.MessageTypeTimeTick {
			assert.GreaterOrEqual(t, msg.TimeTick(), lastTimeTick)
		} else {
			assert.Greater(t, msg.TimeTick(), lastTimeTick)
		}
		lastTimeTick = msg.TimeTick()
		timeticks = append(timeticks, msg.TimeTick())
		if msg.TimeTick() > expectedLastTimeTick {
			break
		}
	}
	msg, err := r1.Next(context.Background())
	// There should be a time tick message.
	assert.NoError(t, err)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())

	// Read from half of the timetick
	<-ch
	assert.Equal(t, totalCnt, len(timeticks)+1)

	targetTimeTickIdx := len(timeticks) / 2
	for targetTimeTickIdx < len(timeticks) && timeticks[targetTimeTickIdx+1] == timeticks[targetTimeTickIdx] {
		targetTimeTickIdx++
	}

	targetTimeTick := timeticks[targetTimeTickIdx]
	r2, err := wb.ReadFromExclusiveTimeTick(context.Background(), targetTimeTick)
	assert.NoError(t, err)
	assert.NotNil(t, r2)
	lastTimeTick = uint64(0)
	for i := 1; ; i++ {
		msg, err := r2.Next(context.Background())
		assert.NoError(t, err)
		if msg.MessageType() == message.MessageTypeTimeTick {
			assert.GreaterOrEqual(t, msg.TimeTick(), lastTimeTick)
		} else {
			assert.Greater(t, msg.TimeTick(), lastTimeTick)
		}
		lastTimeTick = msg.TimeTick()
		assert.Equal(t, timeticks[targetTimeTickIdx+i], msg.TimeTick())
		if msg.TimeTick() > expectedLastTimeTick {
			break
		}
	}
	msg, err = r2.Next(context.Background())
	// There should be a time tick message.
	assert.NoError(t, err)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())

	rEvicted, err := wb.ReadFromExclusiveTimeTick(context.Background(), 0)
	assert.Nil(t, rEvicted)
	assert.ErrorIs(t, err, ErrEvicted)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = r1.Next(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = r2.Next(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	wb.Append(nil, createTimeTickMessage(timeticks[len(timeticks)-1]+1))
	msg, err = r1.Next(ctx)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())
	assert.NoError(t, err)
	msg, err = r2.Next(ctx)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())
	assert.NoError(t, err)
}

func TestWriteAheadBufferIdleEviction(t *testing.T) {
	manager := NewMaintenanceManager(5 * time.Millisecond)
	defer manager.Close()

	wb := NewWriteAheadBuffer(
		manager,
		"pchannel",
		mlog.With(),
		5*1024*1024,
		20*time.Millisecond,
		createTimeTickMessage(0),
	)
	defer wb.Close()

	msgs := make([]message.ImmutableMessage, 0, 99)
	for i := 1; i < 100; i++ {
		msgs = append(msgs, createInsertMessage(uint64(i)))
	}
	wb.Append(msgs, createTimeTickMessage(99))

	assert.Eventually(t, func() bool {
		wb.cond.L.Lock()
		defer wb.cond.L.Unlock()
		return wb.pendingMessages.Len() == 1
	}, time.Second, 5*time.Millisecond)

	wb.cond.L.Lock()
	defer wb.cond.L.Unlock()
	assert.Equal(t, uint64(99), wb.pendingMessages.buf[0].Message.TimeTick())
}

func TestMaintenanceManagerMaintainsMultipleBuffersAndUnregistersOnClose(t *testing.T) {
	manager := NewMaintenanceManager(5 * time.Millisecond)
	defer manager.Close()

	first := NewWriteAheadBuffer(
		manager,
		"pchannel-1",
		mlog.With(),
		5*1024*1024,
		20*time.Millisecond,
		createTimeTickMessage(0),
	)
	second := NewWriteAheadBuffer(
		manager,
		"pchannel-2",
		mlog.With(),
		5*1024*1024,
		20*time.Millisecond,
		createTimeTickMessage(0),
	)
	defer second.Close()

	first.Append([]message.ImmutableMessage{createInsertMessage(1)}, createTimeTickMessage(1))
	second.Append([]message.ImmutableMessage{createInsertMessage(1)}, createTimeTickMessage(1))

	assert.Eventually(t, func() bool {
		return pendingMessageCount(first) == 1 && pendingMessageCount(second) == 1
	}, time.Second, 5*time.Millisecond)

	first.Close()
	manager.mu.Lock()
	assert.Len(t, manager.buffers, 1)
	_, registered := manager.buffers[second]
	manager.mu.Unlock()
	assert.True(t, registered)

	manager.Close()
	manager.mu.Lock()
	assert.Empty(t, manager.buffers)
	manager.mu.Unlock()
	second.Close()
}

func TestWriteAheadBufferEviction(t *testing.T) {
	wb := NewWriteAheadBuffer(newTestMaintenanceManager(t), "pchannel", mlog.With(), 5*1024*1024, 50*time.Millisecond, createTimeTickMessage(0))
	t.Cleanup(wb.Close)

	msgs := make([]message.ImmutableMessage, 0)
	for i := 1; i < 100; i++ {
		msgs = append(msgs, createInsertMessage(uint64(i)))
	}
	wb.Append(msgs, createTimeTickMessage(99))

	// We can read from 0 to 100 messages
	r, err := wb.ReadFromExclusiveTimeTick(context.Background(), 0)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	msg, err := r.Next(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, msg.TimeTick(), uint64(1))

	msgs = make([]message.ImmutableMessage, 0)
	for i := 100; i < 200; i++ {
		msgs = append(msgs, createInsertMessage(uint64(i)))
	}
	wb.Append(msgs, createTimeTickMessage(199))
	time.Sleep(60 * time.Millisecond)
	wb.Append(nil, createTimeTickMessage(200))
	// wait for expiration.

	lastTimeTick := uint64(0)
	for {
		msg, err := r.Next(context.Background())
		if err != nil {
			assert.ErrorIs(t, err, ErrEvicted)
			break
		}
		if msg.MessageType() == message.MessageTypeTimeTick {
			assert.GreaterOrEqual(t, msg.TimeTick(), lastTimeTick)
		} else {
			assert.Greater(t, msg.TimeTick(), lastTimeTick)
		}
		lastTimeTick = msg.TimeTick()
	}
	assert.Equal(t, uint64(99), lastTimeTick)
}

func createTimeTickMessage(timetick uint64) message.ImmutableMessage {
	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable()
	return msg.WithTimeTick(timetick).IntoImmutableMessage(
		walimplstest.NewTestMessageID(1),
	)
}

func newTestMaintenanceManager(t *testing.T) *MaintenanceManager {
	manager := NewMaintenanceManager(time.Hour)
	t.Cleanup(manager.Close)
	return manager
}

func pendingMessageCount(buffer *WriteAheadBuffer) int {
	buffer.cond.L.Lock()
	defer buffer.cond.L.Unlock()
	return buffer.pendingMessages.Len()
}

func createInsertMessage(timetick uint64) message.ImmutableMessage {
	msg, err := message.NewInsertMessageBuilderV1().
		WithVChannel("vchannel").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return msg.WithTimeTick(timetick).IntoImmutableMessage(
		walimplstest.NewTestMessageID(1),
	)
}
