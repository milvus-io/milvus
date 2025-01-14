package log

import "go.uber.org/zap"

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
