package cgo

type options struct {
	releaser func()
}

type Opt func(*options)

func WithReleaser(releaser func()) Opt {
	return func(o *options) {
		o.releaser = releaser
	}
}
