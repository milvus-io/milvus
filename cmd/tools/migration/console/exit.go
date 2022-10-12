package console

import (
	"os"
)

func ExitWithOption(opts ...ExitOption) {
	c := defaultExitConfig()
	c.apply(opts...)
	if c.abnormal {
		Error(c.msg)
	} else {
		Success(c.msg)
	}
	os.Exit(c.code)
}

func AbnormalExit(backupFinished bool, msg string) {
	opts := []ExitOption{WithAbnormalExit(), WithMsg(msg)}
	if backupFinished {
		opts = append(opts, WithExitCode(FailButBackupFinished))
	} else {
		opts = append(opts, WithExitCode(BackupUnfinished))
	}
	ExitWithOption(opts...)
}

func AbnormalExitIf(err error, backupFinished bool) {
	if err != nil {
		AbnormalExit(backupFinished, err.Error())
	}
}

func NormalExit(msg string) {
	ExitWithOption(WithExitCode(NormalCode), WithMsg(msg))
}

func NormalExitIf(success bool, msg string) {
	if success {
		NormalExit(msg)
	}
}
