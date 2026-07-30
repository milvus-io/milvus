package cgo

func getDefaultOpt() *options {
	return &options{
		name: "unknown",
	}
}

// ErrorMapper translates a failed C future status at a call-specific boundary.
// Most callers should use the default merr.SegcoreError mapping; this hook is for
// established API contracts whose classification depends on the operation.
type ErrorMapper func(code int32, message string) error

type options struct {
	name        string
	errorMapper ErrorMapper
}

// Opt is the option type for future.
type Opt func(*options)

// WithName sets the name of the future.
// Only used for metrics.
func WithName(name string) Opt {
	return func(o *options) {
		o.name = name
	}
}

// WithErrorMapper installs an operation-specific failed-status mapper. It is
// invoked only for non-success CStatus values.
func WithErrorMapper(mapper ErrorMapper) Opt {
	return func(o *options) {
		o.errorMapper = mapper
	}
}
