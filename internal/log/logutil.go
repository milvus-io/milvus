package log

import (
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar/log"

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

func (w *zapWrapper) SubLogger(fields log.Fields) log.Logger {
	return w.WithFields(fields).(log.Logger)
}

func (w *zapWrapper) WithFields(fields log.Fields) log.Entry {
	if len(fields) == 0 {
		return w
	}
	kv := make([]interface{}, 0, 2*len(fields))
	for k, v := range fields {
		kv = append(kv, k, v)
	}
	return &zapWrapper{
		logger: w.logger.Sugar().With(kv...).Desugar(),
	}
}

func (w *zapWrapper) WithField(name string, value interface{}) log.Entry {
	return &zapWrapper{
		logger: w.logger.Sugar().With(name, value).Desugar(),
	}
}

func (w *zapWrapper) WithError(err error) log.Entry {
	return &zapWrapper{
		logger: w.logger.Sugar().With("error", err).Desugar(),
	}
}

func (w *zapWrapper) Debug(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Debug(args...)
}

func (w *zapWrapper) Warn(args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Warn(args...)
}

func (w *zapWrapper) Debugf(format string, args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Debugf(format, args...)
}

func (w *zapWrapper) Warnf(format string, args ...interface{}) {
	w.logger.WithOptions(zap.AddCallerSkip(1)).Sugar().Warnf(format, args...)
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
