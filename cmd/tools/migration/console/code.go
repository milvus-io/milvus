package console

type ErrorCode = int

const (
	NormalCode               ErrorCode = 0
	FailWithBackupUnfinished ErrorCode = 1
	FailButBackupFinished    ErrorCode = 2
	Unexpected               ErrorCode = 100
)
