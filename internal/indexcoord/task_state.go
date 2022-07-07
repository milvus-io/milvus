package indexcoord

type indexTaskState int32

const (
	// when we receive a index task
	indexTaskInit indexTaskState = iota
	// we've sent index task to scheduler, and wait for building index.
	indexTaskInProgress
	// task done, wait to be cleaned
	indexTaskDone
	// index task need to retry.
	indexTaskRetry
	// task has been deleted.
	indexTaskDeleted
)

var TaskStateNames = map[indexTaskState]string{
	0: "Init",
	1: "InProgress",
	2: "Done",
	3: "Retry",
	4: "Deleted",
}

func (x indexTaskState) String() string {
	ret, ok := TaskStateNames[x]
	if !ok {
		return "None"
	}
	return ret
}
