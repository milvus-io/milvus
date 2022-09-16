package querynode

import (
	"container/list"
)

type scheduleReadTaskPolicy func(sqTasks *list.List, targetUsage int32, maxNum int32) ([]readTask, int32)

func defaultScheduleReadPolicy(sqTasks *list.List, targetUsage int32, maxNum int32) ([]readTask, int32) {
	var ret []readTask
	usage := int32(0)
	var next *list.Element
	for e := sqTasks.Front(); e != nil && maxNum > 0; e = next {
		next = e.Next()
		t, _ := e.Value.(readTask)
		tUsage := t.CPUUsage()
		if usage+tUsage > targetUsage {
			break
		}
		usage += tUsage
		sqTasks.Remove(e)
		rateCol.rtCounter.sub(t, readyQueueType)
		ret = append(ret, t)
		maxNum--
	}
	return ret, usage
}
