package blist

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/block"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/rm"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestBList(t *testing.T) {
	mutableBL := NewMutableContinousBlockList(10, createATestMessage(t, 0), nil)
	var immutableBL *ImmutableContinousBlockList
	msgCount := 100
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		appendMutableBlockList(t, mutableBL, msgCount)
		r := mutableBL.Range()
		assert.True(t, r.Begin.EQ(walimplstest.NewTestMessageID(0)))
		assert.Nil(t, r.End)
		immutableBL = mutableBL.IntoImmutable()
		r = mutableBL.Range()
		assert.True(t, r.Begin.EQ(walimplstest.NewTestMessageID(0)))
		assert.True(t, r.End.EQ(walimplstest.NewTestMessageID(int64(msgCount-1))))
		r = immutableBL.Range()
		assert.True(t, r.Begin.EQ(walimplstest.NewTestMessageID(0)))
		assert.True(t, r.End.EQ(walimplstest.NewTestMessageID(int64(msgCount-1))))
	}()
	for i := 0; i < msgCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			testScan(t, mutableBL, walimplstest.NewTestMessageID(int64(i)), msgCount-i)
		}(i)
	}
	wg.Wait()

	// Test immutable and mutable block concurrent read.
	for i := 0; i < msgCount; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			testScan(t, immutableBL, walimplstest.NewTestMessageID(int64(i)), msgCount-i)
		}(i)
		go func(i int) {
			defer wg.Done()
			testScan(t, mutableBL, walimplstest.NewTestMessageID(int64(i)), msgCount-i)
		}(i)
	}

	wg.Add(3)
	go func() {
		defer wg.Done()
		s, err := immutableBL.Read(walimplstest.NewTestMessageID(100))
		assert.ErrorIs(t, err, walcache.ErrNotFound)
		assert.Nil(t, s)
	}()
	go func() {
		defer wg.Done()
		s, err := mutableBL.Read(walimplstest.NewTestMessageID(100))
		assert.ErrorIs(t, err, walcache.ErrNotFound)
		assert.Nil(t, s)
	}()

	go func() {
		defer wg.Done()
		cnt := immutableBL.BlockCount()
		assert.Equal(t, 20, cnt)
		idxes := []int{0, 1, 4, 5, 7, 8, 16, 19}
		blockIDs := make([]int64, 0, len(idxes))
		immutableBL.RangeOver(func(idx int, b *block.ImmutableBlock) bool {
			if idx == idxes[0] {
				blockIDs = append(blockIDs, b.BlockID())
				idxes = idxes[1:]
			}
			return true
		})
		immutableBlockLists := []*ImmutableContinousBlockList{immutableBL}

		for _, id := range blockIDs {
			newList := make([]*ImmutableContinousBlockList, 0, len(immutableBlockLists)+1)
			for _, l := range immutableBlockLists {
				if l2, ok := l.Evict(id); ok {
					newList = append(newList, l2...)
					continue
				}
				newList = append(newList, l)
			}
			immutableBlockLists = newList
		}

		assert.Equal(t, 4, len(immutableBlockLists))

		splittedCount := 0
		for _, bl := range immutableBlockLists {
			splittedCount += bl.BlockCount()
			bl := bl
			wg.Add(1)
			go func() {
				defer wg.Done()
				msgCount := 0
				var startMsgID message.MessageID
				bl.RangeOver(func(_ int, b *block.ImmutableBlock) bool {
					cnt, ok := b.Count()
					assert.True(t, ok)
					msgCount += cnt
					if startMsgID == nil {
						startMsgID = b.Range().Begin
					}
					return true
				})
				testScan(t, bl, startMsgID, msgCount)
			}()
		}
	}()
	wg.Wait()
}

func TestMutableBlockListEvict(t *testing.T) {
	mutableBL := NewMutableContinousBlockList(10, createATestMessage(t, 0), nil)
	msgCount := 100
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		appendMutableBlockList(t, mutableBL, msgCount)
	}()
	go func() {
		defer wg.Done()
		var blockID int64
		for {
			mutableBL.RangeOverImmutable(func(idx int, b *block.ImmutableBlock) bool {
				if idx == 1 {
					blockID = b.BlockID()
					return false
				}
				return true
			})
			if blockID != 0 {
				break
			}
		}

		l, ok := mutableBL.Evict(blockID)
		assert.True(t, ok)
		assert.Len(t, l, 1)
	}()
	wg.Wait()
	r := mutableBL.Range()
	assert.False(t, walimplstest.NewTestMessageID(0).EQ(r.Begin))
}

func appendMutableBlockList(t *testing.T, mutableBL *MutableCountinousBlockList, msgCount int) {
	msgs := make([]message.ImmutableMessage, 0, msgCount)
	for i := 1; i < msgCount; i++ {
		time.Sleep(1 * time.Millisecond)
		msg := createATestMessage(t, int64(i))
		msgs = append(msgs, msg)
		if len(msgs) < rand.Intn(100) {
			continue
		}
		mutableBL.Append(msgs)
		msgs = make([]message.ImmutableMessage, 0, msgCount)
	}
	mutableBL.Append(msgs)
}

func TestError(t *testing.T) {
	// Not Found
	mutableB := NewMutableContinousBlockList(100, createATestMessage(t, 1), nil)

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

func createATestMessage(t *testing.T, i int64) message.ImmutableMessage {
	msgID := walimplstest.NewTestMessageID(i)
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageID().Return(msgID).Maybe()
	msg.EXPECT().Payload().Return([]byte{1, 2}).Maybe()
	return msg
}

func testScan(t *testing.T, b walcache.MessageReader, msgID message.MessageID, expectCnt int) {
	s, err := b.Read(msgID)
	assert.NoError(t, err)
	cnt := 0
	for {
		if err := s.Scan(context.TODO()); err != nil {
			break
		}
		assert.NotNil(t, s.Message())
		cnt++
	}
	assert.Equal(t, expectCnt, cnt)
}

func TestMain(m *testing.M) {
	rm.Init(1000, 0, "")

	m.Run()
}
