package querynode

import (
	"container/list"
)

type scheduleReadTaskPolicy func(sqTasks *list.List, targetUsage int32) ([]readTask, int32)

func defaultScheduleReadPolicy(sqTasks *list.List, targetUsage int32) ([]readTask, int32) {
	var ret []readTask
	usage := int32(0)
	for e := sqTasks.Front(); e != nil; e = e.Next() {
		t, _ := e.Value.(readTask)
		tUsage := t.CPUUsage()
		if usage+tUsage > targetUsage {
			break
		}
		usage += tUsage
		sqTasks.Remove(e)
		ret = append(ret, t)
	}
	return ret, usage
}
