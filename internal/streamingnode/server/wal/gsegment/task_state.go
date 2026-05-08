package gsegment

type taskState int

const (
	taskStateSerializing taskState = iota
	taskStateUploading
	taskStateDone
)
