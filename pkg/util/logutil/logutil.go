// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"context"
	"math"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/constraints"
	"google.golang.org/grpc/grpclog"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	// infoLog indicates Info severity.
	infoLog int = iota
	// warningLog indicates Warning severity.
	warningLog
	// errorLog indicates Error severity.
	errorLog
)

type zapWrapper struct {
	logger   *zap.Logger
	logLevel int
}

// Info logs a message at InfoLevel.
func (w *zapWrapper) Info(args ...interface{}) {
	if infoLog >= w.logLevel {
		w.logger.Sugar().Info(args...)
	}
}

func (w *zapWrapper) Infoln(args ...interface{}) {
	if infoLog >= w.logLevel {
		w.logger.Sugar().Info(args...)
	}
}

func (w zapWrapper) Infof(format string, args ...interface{}) {
	if infoLog >= w.logLevel {
		w.logger.Sugar().Infof(format, args...)
	}
}

func (w zapWrapper) Warning(args ...interface{}) {
	if warningLog >= w.logLevel {
		w.logger.Sugar().Warn(args...)
	}
}

func (w zapWrapper) Warningln(args ...interface{}) {
	if warningLog >= w.logLevel {
		w.logger.Sugar().Warn(args...)
	}
}

func (w *zapWrapper) Warningf(format string, args ...interface{}) {
	if warningLog >= w.logLevel {
		w.logger.Sugar().Warnf(format, args...)
	}
}

func (w zapWrapper) Error(args ...interface{}) {
	if errorLog >= w.logLevel {
		w.logger.Sugar().Error(args...)
	}
}

func (w *zapWrapper) Errorln(args ...interface{}) {
	if errorLog >= w.logLevel {
		w.logger.Sugar().Error(args...)
	}
}

func (w zapWrapper) Errorf(format string, args ...interface{}) {
	if errorLog >= w.logLevel {
		w.logger.Sugar().Errorf(format, args...)
	}
}

func (w *zapWrapper) Fatal(args ...interface{}) {
	w.logger.Sugar().Fatal(args...)
}

func (w zapWrapper) Fatalln(args ...interface{}) {
	w.logger.Sugar().Fatal(args...)
}

func (w *zapWrapper) Fatalf(format string, args ...interface{}) {
	w.logger.Sugar().Fatalf(format, args...)
}

// V reports whether verbosity level l is at least the requested verbose level.
// grpc LoggerV2
// 0=info, 1=warning, 2=error, 3=fatal
// zap
// -1=debug, 0=info, 1=warning, 2=error, 3=dpanic, 4=panic, 5=fatal
func (w *zapWrapper) V(l int) bool {
	if l < w.logLevel {
		return false
	}

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
		mlog.Fatal(context.TODO(), "panic", mlog.Reflect("recover", e))
	}
}

var once sync.Once

// SetupLogger is used to initialize the log with config.
func SetupLogger(cfg *mlog.Config) {
	once.Do(func() {
		// Initialize logger.
		logger, p, err := mlog.InitLogger(cfg, zap.AddStacktrace(zap.ErrorLevel))
		if err == nil {
			mlog.ReplaceGlobals(logger, p)
		} else {
			mlog.Fatal(context.TODO(), "initialize logger error", mlog.Err(err))
		}

		// Initialize grpc log wrapper
		logLevel := 0
		switch cfg.GrpcLevel {
		case "", "ERROR": // If env is unset, set level to ERROR.
			logLevel = 2
		case "WARNING":
			logLevel = 1
		case "INFO":
			logLevel = 0
		}

		wrapper := &zapWrapper{logger, logLevel}
		grpclog.SetLoggerV2(wrapper)

		mlog.Info(context.TODO(), "Log directory", mlog.String("configDir", cfg.File.RootPath))
		mlog.Info(context.TODO(), "Set log file to ", mlog.String("path", cfg.File.Filename))
	})
}

func Logger(ctx context.Context) *zap.Logger {
	return mlog.L().With(mlog.FieldsFromContext(ctx)...)
}

func WithModule(ctx context.Context, module string) context.Context {
	return mlog.WithModule(ctx, module)
}

// keeps only 2 decimal places
func ToMB[T constraints.Integer | constraints.Float](mem T) T {
	return T(math.Round(float64(mem)/1024/1024*100) / 100)
}
