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

	actual := int32(180)     // sum(3..6) * 10   3 + 4 + 5 + 6
	targetUsage = int32(190) // > actual
	maxNum = math.MaxInt32
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, maxNum)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 4, len(tasks))

	actual = 340 // sum(7..10) * 10 ,  7+ 8 + 9 + 10
	targetUsage = 340
	maxNum = 4
	tasks, cur = scheduleFunc(readyReadTasks, targetUsage, maxNum)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 4, len(tasks))
}
