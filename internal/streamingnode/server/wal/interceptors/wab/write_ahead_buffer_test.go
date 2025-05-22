package wab

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestWriteAheadBufferWithOnlyTrivialTimeTick(t *testing.T) {
	ctx := context.Background()
	wb := NewWriteAheadBuffer("pchannel", log.With(), 5*1024*1024, 30*time.Second, createTimeTickMessage(0, true))

	// Test timeout
	ctx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancel()
	r, err := wb.ReadFromExclusiveTimeTick(ctx, 100)
	assert.Nil(t, r)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	readErr := syncutil.NewFuture[struct{}]()
	expectedLastTimeTick := uint64(10000)
	go func() {
		r, err := wb.ReadFromExclusiveTimeTick(context.Background(), 100)
		assert.NoError(t, err)
		assert.NotNil(t, r)
		lastTimeTick := uint64(0)
		for {
			msg, err := r.Next(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, msg)
			assert.Greater(t, msg.TimeTick(), lastTimeTick)
			lastTimeTick = msg.TimeTick()
			if msg.TimeTick() > expectedLastTimeTick {
				break
			}
		}
		// Because there's no more message updated, so the Next operation should be blocked forever.
		ctx, cancel = context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		msg, err := r.Next(ctx)
		assert.Nil(t, msg)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		readErr.Set(struct{}{})
	}()

	// Current the cache last timetick will be push to 100,
	// But we make a exclusive read, so the read operation should be blocked.
	wb.Append(nil, createTimeTickMessage(100, false))
	ctx, cancel = context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	_, err = readErr.GetWithContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	nextTimeTick := uint64(100)
	for {
		nextTimeTick += uint64(rand.Int31n(1000) + 1)
		wb.Append(nil, createTimeTickMessage(nextTimeTick, false))
		if nextTimeTick > expectedLastTimeTick {
			break
		}
	}
	readErr.Get()

	r, err = wb.ReadFromExclusiveTimeTick(context.Background(), 0)
	assert.NoError(t, err)
	msg, err := r.Next(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())
	assert.Equal(t, nextTimeTick, msg.TimeTick())
}

func TestWriteAheadBuffer(t *testing.T) {
	// Concurrent add message into bufffer and make syncup.
	// The reader should never lost any message if no eviction happen.
	wb := NewWriteAheadBuffer("pchannel", log.With(), 5*1024*1024, 30*time.Second, createTimeTickMessage(1, true))
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
			wb.Append(msgs, createTimeTickMessage(msgs[len(msgs)-1].TimeTick(), true))
			totalCnt += (len(msgs) + 1)
			if nextTimeTick > expectedLastTimeTick {
				break
			}
		}
	}()
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
	assert.Equal(t, totalCnt, len(timeticks))

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
	wb.Append(nil, createTimeTickMessage(timeticks[len(timeticks)-1]+1, false))
	msg, err = r1.Next(ctx)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())
	assert.NoError(t, err)
	msg, err = r2.Next(ctx)
	assert.Equal(t, message.MessageTypeTimeTick, msg.MessageType())
	assert.NoError(t, err)
}

func TestWriteAheadBufferEviction(t *testing.T) {
	wb := NewWriteAheadBuffer("pchannel", log.With(), 5*1024*1024, 50*time.Millisecond, createTimeTickMessage(0, true))

	msgs := make([]message.ImmutableMessage, 0)
	for i := 1; i < 100; i++ {
		msgs = append(msgs, createInsertMessage(uint64(i)))
	}
	wb.Append(msgs, createTimeTickMessage(99, true))

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
	wb.Append(msgs, createTimeTickMessage(199, true))
	time.Sleep(60 * time.Millisecond)
	wb.Append(nil, createTimeTickMessage(200, false))
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

func createTimeTickMessage(timetick uint64, persist bool) message.ImmutableMessage {
	b := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{})
	if !persist {
		b.WithNotPersisted()
	}
	msg := b.MustBuildMutable()
	return msg.WithTimeTick(timetick).IntoImmutableMessage(
		walimplstest.NewTestMessageID(1),
	)
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
