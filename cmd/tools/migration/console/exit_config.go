package console

type exitConfig struct {
	abnormal  bool
	code      ErrorCode
	msg       string
	callbacks []func()
}

func defaultExitConfig() exitConfig {
	return exitConfig{
		abnormal:  false,
		code:      NormalCode,
		msg:       "",
		callbacks: make([]func(), 0),
	}
}

type ExitOption func(c *exitConfig)

func (c *exitConfig) apply(opts ...ExitOption) {
	for _, opt := range opts {
		opt(c)
	}
}

func (c *exitConfig) runBeforeExit() {
	for _, cb := range c.callbacks {
		cb()
	}
}

func WithExitCode(code ErrorCode) ExitOption {
	return func(c *exitConfig) {
		c.code = code
	}
}

func WithAbnormalExit() ExitOption {
	return func(c *exitConfig) {
		c.abnormal = true
	}
}

func WithMsg(msg string) ExitOption {
	return func(c *exitConfig) {
		c.msg = msg
	}
}

func AddCallbacks(fns ...func()) ExitOption {
	return func(c *exitConfig) {
		c.callbacks = append(c.callbacks, fns...)
	}
}
