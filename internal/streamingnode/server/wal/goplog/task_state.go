package goplog

type taskState int

const (
	taskStateInit taskState = iota
	taskStateSerializing
	taskStateUploading
	taskStateDone
)
