// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package logutil

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"

	"go.uber.org/zap/zapcore"

	clientv3 "go.etcd.io/etcd/client/v3"
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

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic() {
	if e := recover(); e != nil {
		log.Fatal("panic", zap.Reflect("recover", e))
	}
}

var once sync.Once
var _globalZapWrapper atomic.Value

const defaultLogLevel = "info"

func init() {
	conf := &log.Config{Level: defaultLogLevel, File: log.FileLogConfig{}}
	lg, _, _ := log.InitLogger(conf)
	_globalZapWrapper.Store(&zapWrapper{
		logger: lg,
	})
}

// SetupLogger is used to initialize the log with config.
func SetupLogger(cfg *log.Config) {
	once.Do(func() {
		// initialize logger
		logger, p, err := log.InitLogger(cfg, zap.AddStacktrace(zap.ErrorLevel))
		if err == nil {
			log.ReplaceGlobals(logger, p)
		} else {
			log.Fatal("initialize logger error", zap.Error(err))
		}

		// initialize grpc and etcd logger
		c := *cfg
		c.Level = defaultLogLevel
		lg, _, err := log.InitLogger(&c)
		if err != nil {
			log.Fatal("initialize grpc/etcd logger error", zap.Error(err))
		}

		wrapper := &zapWrapper{lg}
		grpclog.SetLoggerV2(wrapper)
		clientv3.SetLogger(wrapper)
		_globalZapWrapper.Store(wrapper)
	})
}

// GetZapWrapper returns the stored zapWrapper object.
func GetZapWrapper() *zapWrapper {
	return _globalZapWrapper.Load().(*zapWrapper)
}
