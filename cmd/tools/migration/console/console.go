package console

import (
	"fmt"
	"os"
)

const (
	ansiGreen  = "\033[32m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiReset  = "\033[0m"
)

func Success(msg string) {
	colorOut(msg, ansiGreen)
}

func Error(msg string) {
	colorOut(msg, ansiRed)
}

func Warning(msg string) {
	colorOut(msg, ansiYellow)
}

func Exit(msg string, options ...ExitOption) {
	opts := append([]ExitOption{}, options...)
	opts = append(opts, WithAbnormalExit(), WithExitCode(Unexpected), WithMsg(msg))
	ExitWithOption(opts...)
}

func ExitIf(err error, options ...ExitOption) {
	if err != nil {
		Exit(err.Error(), options...)
	}
}

func ErrorExitIf(fail bool, backupFinished bool, msg string) {
	if fail {
		AbnormalExit(backupFinished, msg)
	}
}

func colorOut(message, color string) {
	fmt.Fprintln(os.Stdout, color+message+ansiReset)
}
