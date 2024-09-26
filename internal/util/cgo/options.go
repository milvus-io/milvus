package cgo

func getDefaultOpt() *options {
	return &options{
		name: "unknown",
	}
}

type options struct {
	name string
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
