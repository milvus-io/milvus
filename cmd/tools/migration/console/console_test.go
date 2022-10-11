package console

import (
	"testing"
)

func TestSuccess(t *testing.T) {
	Success("success")
}

func TestError(t *testing.T) {
	Error("error")
}

func TestWarning(t *testing.T) {
	Warning("warning")
}

func TestExitIf(t *testing.T) {
	ExitIf(nil)
}
