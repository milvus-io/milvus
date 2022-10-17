package console

type ErrorCode = int

const (
	NormalCode            ErrorCode = 0
	BackupUnfinished      ErrorCode = 1
	FailButBackupFinished ErrorCode = 2
	Unexpected            ErrorCode = 100
)
