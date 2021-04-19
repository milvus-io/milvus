package log

import (
	"sync"
	"sync/atomic"

	"go.uber.org/zap/zapcore"

	etcd "go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc/grpclog"

	"go.uber.org/zap"
)

type zapWrapper struct {
	logger *zap.Logger
}

func (w *zapWrapper) Info(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Info(args...)
}

func (w *zapWrapper) Infoln(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Info(args...)
}

func (w zapWrapper) Infof(format string, args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Infof(format, args...)
}

func (w zapWrapper) Warning(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Warn(args...)
}

func (w zapWrapper) Warningln(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Warn(args...)
}

func (w *zapWrapper) Warningf(format string, args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Warnf(format, args...)
}

func (w zapWrapper) Error(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Error(args...)
}

func (w *zapWrapper) Errorln(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Error(args...)
}

func (w zapWrapper) Errorf(format string, args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Errorf(format, args...)
}

func (w *zapWrapper) Fatal(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Fatal(args...)
}

func (w zapWrapper) Fatalln(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Fatal(args...)
}

func (w *zapWrapper) Fatalf(format string, args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Fatalf(format, args...)
}

// V reports whether verbosity level l is at least the requested verbose level.
// grpc LoggerV2
// 0=info, 1=warning, 2=error, 3=fatal
// zap
// -1=debug, 0=info, 1=warning, 2=error, 3=dpanic, 4=panic, 5=fatal
func (w *zapWrapper) V(l int) bool {
	zapLevel := l
	if l == 3 {
		zapLevel = 5
	}
	return w.logger.Core().Enabled(zapcore.Level(zapLevel))
}

var once sync.Once
var _globalZapWrapper atomic.Value

const defaultLogLevel = "info"

func init() {
	conf := &Config{Level: defaultLogLevel, File: FileLogConfig{}}
	lg, _, _ := InitLogger(conf)
	_globalZapWrapper.Store(&zapWrapper{
		logger: lg,
	})
}

func SetupLogger(cfg *Config) {
	once.Do(func() {
		// initialize logger
		logger, p, err := InitLogger(cfg, zap.AddStacktrace(zap.ErrorLevel))
		if err == nil {
			ReplaceGlobals(logger, p)
		} else {
			Fatal("initialize logger error", zap.Error(err))
		}

		// initialize grpc and etcd logger
		c := *cfg
		c.Level = defaultLogLevel
		lg, _, err := InitLogger(&c)
		if err != nil {
			Fatal("initialize grpc/etcd logger error", zap.Error(err))
		}

		wrapper := &zapWrapper{lg}
		grpclog.SetLoggerV2(wrapper)
		etcd.SetLogger(wrapper)
		_globalZapWrapper.Store(wrapper)
	})
}

func GetZapWrapper() *zapWrapper {
	return _globalZapWrapper.Load().(*zapWrapper)
}
