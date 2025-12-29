package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	FieldNameModule    = "module"
	FieldNameComponent = "component"
)

// FieldModule returns a zap field with the module name.
func FieldModule(module string) zap.Field {
	return zap.String(FieldNameModule, module)
}

// FieldComponent returns a zap field with the component name.
func FieldComponent(component string) zap.Field {
	return zap.String(FieldNameComponent, component)
}

// FieldMessage returns a zap field with the message object.
func FieldMessage(msg zapcore.ObjectMarshaler) zap.Field {
	return zap.Object("message", msg)
}

// FieldMessages returns a zap array field with the messages.
func FieldMessages[T zapcore.ObjectMarshaler](msgs []T) zap.Field {
	return zap.Objects("messages", msgs)
}
