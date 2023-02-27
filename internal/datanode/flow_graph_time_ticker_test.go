package datanode

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestMergedTimeTicker(t *testing.T) {
	var ticks []uint64
	var mut sync.Mutex

	mt := newMergedTimeTickerSender(func(ts Timestamp, _ []int64) error {
		mut.Lock()
		defer mut.Unlock()
		ticks = append(ticks, ts)
		return nil
	})

	for i := 1; i < 100; i++ {
		time.Sleep(time.Millisecond * 10)
		mt.bufferTs(uint64(i), nil)
	}
	mt.close()
	mut.Lock()
	assert.EqualValues(t, 99, ticks[len(ticks)-1])
	assert.Less(t, len(ticks), 20)
	mut.Unlock()
}

func TestMergedTimeTicker_close10000(t *testing.T) {
	var wg sync.WaitGroup
	batchSize := 10000
	wg.Add(batchSize)
	for i := 0; i < batchSize; i++ {
		mt := newMergedTimeTickerSender(func(ts Timestamp, _ []int64) error {
			return nil
		})
		go func(mt *mergedTimeTickerSender) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			mt.close()
		}(mt)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("wait all timer close, timeout")
	case <-done:
	}
}

func TestMergedTimeTickerSendFail(t *testing.T) {
	var ticks []uint64
	var mut sync.Mutex
	first := true
	mt := newMergedTimeTickerSender(func(ts Timestamp, _ []int64) error {
		mut.Lock()
		defer mut.Unlock()
		if first {
			first = false
			return errors.New("merged time tick")
		}
		ticks = append(ticks, ts)
		return nil
	})

	for i := 1; i < 100; i++ {
		time.Sleep(time.Millisecond * 10)
		mt.bufferTs(uint64(i), nil)
	}
	mt.close()
	mut.Lock()
	assert.EqualValues(t, 99, ticks[len(ticks)-1])
	assert.Less(t, len(ticks), 20)
	mut.Unlock()
}
