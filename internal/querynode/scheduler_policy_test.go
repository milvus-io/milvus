package querynode

import (
	"container/list"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScheduler_defaultScheduleReadPolicy(t *testing.T) {
	readyReadTasks := list.New()
	for i := 1; i <= 10; i++ {
		t := mockReadTask{
			cpuUsage: int32(i * 10),
		}
		readyReadTasks.PushBack(&t)
	}

	scheduleFunc := defaultScheduleReadPolicy

	targetUsage := int32(100)
	maxNum := int32(2)

	tasks, cur := scheduleFunc(readyReadTasks, targetUsage, maxNum)
	assert.Equal(t, int32(30), cur)
	assert.Equal(t, int32(2), int32(len(tasks)))

	targetUsage = 300
	maxNum = 0
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, maxNum)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))

	targetUsage = 0
	maxNum = 0
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, maxNum)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))

	targetUsage = 0
	maxNum = 300
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, maxNum)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))

	targetUsage = int32(190) // > actual
	actual := int32(250)     // sum(3..7) * 10   3 + 4 + 5 + 6 + 7
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, math.MaxInt32)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 5, len(tasks))

	actual = 270 // sum(8..10) * 10 ,  8 + 9 + 10
	targetUsage = 340
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, math.MaxInt32)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 3, len(tasks))
}
