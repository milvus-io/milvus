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

func Exit(msg string) {
	ExitWithOption(WithAbnormalExit(), WithMsg(msg))
}

func ExitIf(err error) {
	if err != nil {
		Exit(err.Error())
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
