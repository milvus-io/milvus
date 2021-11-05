package datanode

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMergedTimeTicker(t *testing.T) {
	var ticks []uint64
	var mut sync.Mutex

	mt := newMergedTimeTickerSender(func(ts Timestamp) error {
		mut.Lock()
		defer mut.Unlock()
		ticks = append(ticks, ts)
		return nil
	})

	for i := 1; i < 100; i++ {
		time.Sleep(time.Millisecond * 10)
		mt.bufferTs(uint64(i))
	}
	mt.close()
	mut.Lock()
	assert.EqualValues(t, 99, ticks[len(ticks)-1])
	assert.Less(t, len(ticks), 20)
	mut.Unlock()
}
