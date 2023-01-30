package mysqld

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
	"go.uber.org/zap/zapcore"
)

type Server struct {
	listener *driver.Listener
}

func (s *Server) Start() error {
	go s.listener.Accept()
	return nil
}

func (s *Server) Close() error {
	s.listener.Close()
	return nil
}

func NewServer(s types.ProxyComponent, port int, level zapcore.Level) (*Server, error) {
	var l = xlog.NewStdLog(xlog.Level(xlog.INFO))
	switch level {
	case zapcore.DebugLevel:
		l = xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	case zapcore.InfoLevel:
		l = xlog.NewStdLog(xlog.Level(xlog.INFO))
	case zapcore.WarnLevel:
		l = xlog.NewStdLog(xlog.Level(xlog.WARNING))
	case zapcore.ErrorLevel:
		l = xlog.NewStdLog(xlog.Level(xlog.ERROR))
	case zapcore.FatalLevel:
		l = xlog.NewStdLog(xlog.Level(xlog.FATAL))
	case zapcore.PanicLevel:
		l = xlog.NewStdLog(xlog.Level(xlog.PANIC))
	}
	addr := fmt.Sprintf(":%d", port)
	h := newHandler(s)
	listener, err := driver.NewListener(l, addr, h)
	if err != nil {
		return nil, err
	}
	r := &Server{listener: listener}
	return r, nil
}
