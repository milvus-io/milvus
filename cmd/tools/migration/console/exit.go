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
	c.runBeforeExit()
	os.Exit(c.code)
}

func AbnormalExit(backupFinished bool, msg string, options ...ExitOption) {
	opts := append([]ExitOption{}, options...)
	opts = append(opts, WithAbnormalExit())
	opts = append(opts, WithMsg(msg))
	if backupFinished {
		opts = append(opts, WithExitCode(FailButBackupFinished))
	} else {
		opts = append(opts, WithExitCode(FailWithBackupUnfinished))
	}
	ExitWithOption(opts...)
}

func AbnormalExitIf(err error, backupFinished bool, options ...ExitOption) {
	if err != nil {
		AbnormalExit(backupFinished, err.Error(), options...)
	}
}

func NormalExit(msg string, options ...ExitOption) {
	opts := append([]ExitOption{}, options...)
	opts = append(opts, WithExitCode(NormalCode), WithMsg(msg))
	ExitWithOption(opts...)
}

func NormalExitIf(success bool, msg string, options ...ExitOption) {
	if success {
		NormalExit(msg, options...)
	}
}
