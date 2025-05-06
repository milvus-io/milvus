package log

import (
	"testing"
)

func TestLoggerBinder(t *testing.T) {
	type b struct {
		Binder
	}

	bb := b{}
	bb.Logger().Debug("test")

	bb.SetLogger(With(FieldModule("test"), FieldComponent("test"), FieldMessage(username("123"))))
	bb.Logger().Debug("test")
	bb.SetLogger(nil)
	bb.Logger().Debug("test")
}
