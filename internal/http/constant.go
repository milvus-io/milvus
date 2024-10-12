package http

import (
	"time"
)

const (
	HTTPHeaderAllowInt64     = "Accept-Type-Allow-Int64"
	HTTPHeaderRequestTimeout = "Request-Timeout"
	HTTPDefaultTimeout       = 30 * time.Second
	HTTPReturnCode           = "code"
	HTTPReturnMessage        = "message"
	HTTPReturnData           = "data"
)
