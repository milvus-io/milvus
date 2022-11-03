package console

import (
	"fmt"
	"os"

	"github.com/mgutz/ansi"
)

func Success(msg string) {
	colorOut(msg, "green")
}

func Error(msg string) {
	colorOut(msg, "red")
}

func Warning(msg string) {
	colorOut(msg, "yellow")
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
	fmt.Fprintln(os.Stdout, ansi.Color(message, color))
}
