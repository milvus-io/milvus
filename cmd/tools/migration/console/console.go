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
	Error(msg)
	os.Exit(1)
}

func ExitIf(err error) {
	if err != nil {
		Exit(err.Error())
	}
}

func NormalExit(msg string) {
	Success(msg)
	os.Exit(0)
}

func NormalExitIf(success bool, msg string) {
	if success {
		NormalExit(msg)
	}
}

func ErrorExit(msg string) {
	Warning(msg)
	os.Exit(1)
}

func ErrorExitIf(fail bool, msg string) {
	if fail {
		ErrorExit(msg)
	}
}

func colorOut(message, color string) {
	fmt.Fprintln(os.Stdout, ansi.Color(message, color))
}
