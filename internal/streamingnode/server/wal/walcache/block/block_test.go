package block

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/rm"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestMemBlockImpl(t *testing.T) {
	rm.Init(1000, 0, "")

	// Test mutable block concurrent read write.
	mutableB := NewMutableBlock(200, createATestMessage(t, 0), nil)
	var immutableB *ImmutableBlock
	msgCount := 100
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgs := make([]message.ImmutableMessage, 0, msgCount)
		for i := 1; i < msgCount; i++ {
			time.Sleep(1 * time.Millisecond)
			msg := createATestMessage(t, int64(i))
			msgs = append(msgs, msg)
			if len(msgs) < rand.Intn(10) {
				continue
			}
			n := mutableB.Append(msgs)
			rest := msgs[n:]
			assert.Empty(t, rest)
			sz, ok := mutableB.Size()
			assert.False(t, ok)
			assert.Equal(t, (i+1)*2, sz)
			cnt, ok := mutableB.Count()
			assert.False(t, ok)
			assert.Equal(t, (i + 1), cnt)
			msgs = make([]message.ImmutableMessage, 0, msgCount)
		}
		mutableB.Append(msgs)
		msg := createATestMessage(t, int64(101))
		n := mutableB.Append([]message.ImmutableMessage{msg})
		assert.Equal(t, 0, n)
		immutableB = mutableB.IntoImmutable()
	}()
	for i := 0; i < msgCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			testScan(t, mutableB, walimplstest.NewTestMessageID(int64(i)), msgCount-i)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 100 is not in the mutable block [0, 99].
		// so a ErrNotReach error will be returned.
		scanner, err := mutableB.Read(walimplstest.NewTestMessageID(100))
		assert.NoError(t, err)
		assert.ErrorIs(t, scanner.Scan(context.Background()), walcache.ErrNotReach)
	}()
	wg.Wait()

	sz, ok := immutableB.Size()
	assert.Equal(t, 2*msgCount, sz)
	assert.True(t, ok)

	// Test immutable and mutable block concurrent read.
	for i := 0; i < msgCount; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			testScan(t, immutableB, walimplstest.NewTestMessageID(int64(i)), msgCount-i)
		}(i)
		go func(i int) {
			defer wg.Done()
			testScan(t, mutableB, walimplstest.NewTestMessageID(int64(i)), msgCount-i)
		}(i)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		s, err := immutableB.Read(walimplstest.NewTestMessageID(100))
		assert.ErrorIs(t, err, walcache.ErrNotFound)
		assert.Nil(t, s)
	}()
	go func() {
		defer wg.Done()
		s, err := mutableB.Read(walimplstest.NewTestMessageID(100))
		assert.ErrorIs(t, err, walcache.ErrNotFound)
		assert.Nil(t, s)
	}()
	wg.Wait()
}

func TestErrors(t *testing.T) {
	rm.Init(1000, 0, "")
	// Not Found
	mutableB := NewMutableBlock(100, createATestMessage(t, 1), nil)

	scanner, err := mutableB.Read(walimplstest.NewTestMessageID(0))
	assert.ErrorIs(t, err, walcache.ErrNotFound)
	assert.Nil(t, scanner)

	immutable := mutableB.IntoImmutable()

	scanner, err = mutableB.Read(walimplstest.NewTestMessageID(0))
	assert.ErrorIs(t, err, walcache.ErrNotFound)
	assert.Nil(t, scanner)

	scanner, err = immutable.Read(walimplstest.NewTestMessageID(0))
	assert.ErrorIs(t, err, walcache.ErrNotFound)
	assert.Nil(t, scanner)

	// Append on sealed mutable block should panics
	assert.Panics(t, func() {
		mutableB.Append([]message.ImmutableMessage{createATestMessage(t, 2)})
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scanner, err = immutable.Read(walimplstest.NewTestMessageID(1))
	assert.NoError(t, err)
	assert.NotNil(t, scanner)
	assert.ErrorIs(t, scanner.Scan(ctx), context.Canceled)

	scanner, err = mutableB.Read(walimplstest.NewTestMessageID(1))
	assert.NoError(t, err)
	assert.NotNil(t, scanner)
	assert.ErrorIs(t, scanner.Scan(ctx), context.Canceled)
}

func TestMemTimeoutImpl(t *testing.T) {
	mutableB := NewMutableBlock(100, createATestMessage(t, 0), nil)
	scanner, err := mutableB.Read(walimplstest.NewTestMessageID(100))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = scanner.Scan(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	msgID := walimplstest.NewTestMessageID(1)
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageID().Return(msgID).Maybe()
	msg.EXPECT().Payload().Return([]byte{1, 2})
	mutableB.Append([]message.ImmutableMessage{msg})

	scanner, err = mutableB.Read(walimplstest.NewTestMessageID(1))
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = scanner.Scan(ctx)
	assert.NoError(t, err)
	err = scanner.Scan(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func testScan(t *testing.T, b Block, msgID message.MessageID, expectCnt int) {
	s, err := b.Read(msgID)
	assert.NoError(t, err)
	cnt := 0
	for s.Scan(context.TODO()) == nil {
		assert.NotNil(t, s.Message())
		cnt++
	}
	assert.Equal(t, cnt, expectCnt)
}

func createATestMessage(t *testing.T, i int64) message.ImmutableMessage {
	msgID := walimplstest.NewTestMessageID(i)
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageID().Return(msgID).Maybe()
	msg.EXPECT().Payload().Return([]byte{1, 2}).Maybe()
	return msg
}
