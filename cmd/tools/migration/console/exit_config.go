package console

type exitConfig struct {
	abnormal bool
	code     ErrorCode
	msg      string
}

func defaultExitConfig() exitConfig {
	return exitConfig{abnormal: false, code: 0, msg: ""}
}

type ExitOption func(c *exitConfig)

func (c *exitConfig) apply(opts ...ExitOption) {
	for _, opt := range opts {
		opt(c)
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
