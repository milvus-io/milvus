package connection

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_priorityQueue(t *testing.T) {
	q := newPriorityQueue()
	repeat := 10
	for i := 0; i < repeat; i++ {
		heap.Push(&q, newQueryItem(int64(i), time.Now()))
	}
	counter := repeat - 1
	for q.Len() > 0 {
		item := heap.Pop(&q).(*queueItem)
		assert.Equal(t, int64(counter), item.identifier)
		counter--
	}
}
