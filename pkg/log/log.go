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

// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/uber/jaeger-client-go/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"gopkg.in/natefinch/lumberjack.v2"
)

var _globalL, _globalP, _globalS, _globalR atomic.Value

var (
	_globalLevelLogger sync.Map
	_namedRateLimiters sync.Map
)

func init() {
	l, p := newStdLogger()

	replaceLeveledLoggers(l)
	_globalL.Store(l)
	_globalP.Store(p)

	s := _globalL.Load().(*zap.Logger).Sugar()
	_globalS.Store(s)

	r := utils.NewRateLimiter(1.0, 60.0)
	_globalR.Store(r)
}

// InitLogger initializes a zap logger.
func InitLogger(cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	var outputs []zapcore.WriteSyncer
	if len(cfg.File.Filename) > 0 {
		lg, err := initFileLog(&cfg.File)
		if err != nil {
			return nil, nil, err
		}
		outputs = append(outputs, zapcore.AddSync(lg))
	}
	if cfg.Stdout {
		stdOut, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return nil, nil, err
		}
		outputs = append(outputs, stdOut)
	}
	debugCfg := *cfg
	debugCfg.Level = "debug"
	outputsWriter := zap.CombineWriteSyncers(outputs...)
	debugL, r, err := InitLoggerWithWriteSyncer(&debugCfg, outputsWriter, opts...)
	if err != nil {
		return nil, nil, err
	}
	replaceLeveledLoggers(debugL)
	level := zapcore.DebugLevel
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		return nil, nil, err
	}
	r.Level.SetLevel(level)
	return debugL.WithOptions(zap.AddCallerSkip(1)), r, nil
}

// InitTestLogger initializes a logger for unit tests
func InitTestLogger(t zaptest.TestingT, cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	writer := newTestingWriter(t)
	zapOptions := []zap.Option{
		// Send zap errors to the same writer and mark the test as failed if
		// that happens.
		zap.ErrorOutput(writer.WithMarkFailed(true)),
	}
	opts = append(zapOptions, opts...)
	return InitLoggerWithWriteSyncer(cfg, writer, opts...)
}

// InitLoggerWithWriteSyncer initializes a zap logger with specified  write syncer.
func InitLoggerWithWriteSyncer(cfg *Config, output zapcore.WriteSyncer, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, nil, fmt.Errorf("initLoggerWithWriteSyncer UnmarshalText cfg.Level err:%w", err)
	}
	core := NewTextCore(newZapTextEncoder(cfg), output, level)
	opts = append(cfg.buildOptions(output), opts...)
	lg := zap.New(core, opts...)
	r := &ZapProperties{
		Core:   core,
		Syncer: output,
		Level:  level,
	}
	return lg, r, nil
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	logPath := strings.Join([]string{cfg.RootPath, cfg.Filename}, string(filepath.Separator))
	if st, err := os.Stat(logPath); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	return &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}

func newStdLogger() (*zap.Logger, *ZapProperties) {
	conf := &Config{Level: "debug", Stdout: true, DisableErrorVerbose: true}
	lg, r, _ := InitLogger(conf, zap.OnFatal(zapcore.WriteThenPanic))
	return lg, r
}

// L returns the global Logger, which can be reconfigured with ReplaceGlobals.
// It's safe for concurrent use.
func L() *zap.Logger {
	return _globalL.Load().(*zap.Logger)
}

// S returns the global SugaredLogger, which can be reconfigured with
// ReplaceGlobals. It's safe for concurrent use.
func S() *zap.SugaredLogger {
	return _globalS.Load().(*zap.SugaredLogger)
}

// R returns utils.ReconfigurableRateLimiter.
func R() *utils.ReconfigurableRateLimiter {
	return _globalR.Load().(*utils.ReconfigurableRateLimiter)
}

func ctxL() *zap.Logger {
	level := _globalP.Load().(*ZapProperties).Level.Level()
	l, ok := _globalLevelLogger.Load(level)
	if !ok {
		return L()
	}
	return l.(*zap.Logger)
}

func debugL() *zap.Logger {
	v, _ := _globalLevelLogger.Load(zapcore.DebugLevel)
	return v.(*zap.Logger)
}

func infoL() *zap.Logger {
	v, _ := _globalLevelLogger.Load(zapcore.InfoLevel)
	return v.(*zap.Logger)
}

func warnL() *zap.Logger {
	v, _ := _globalLevelLogger.Load(zapcore.WarnLevel)
	return v.(*zap.Logger)
}

func errorL() *zap.Logger {
	v, _ := _globalLevelLogger.Load(zapcore.ErrorLevel)
	return v.(*zap.Logger)
}

func fatalL() *zap.Logger {
	v, _ := _globalLevelLogger.Load(zapcore.FatalLevel)
	return v.(*zap.Logger)
}

// ReplaceGlobals replaces the global Logger and SugaredLogger.
// It's safe for concurrent use.
func ReplaceGlobals(logger *zap.Logger, props *ZapProperties) {
	_globalL.Store(logger)
	_globalS.Store(logger.Sugar())
	_globalP.Store(props)
}

func replaceLeveledLoggers(debugLogger *zap.Logger) {
	levels := []zapcore.Level{
		zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel,
		zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel,
	}
	for _, level := range levels {
		levelL := debugLogger.WithOptions(zap.IncreaseLevel(level))
		_globalLevelLogger.Store(level, levelL)
	}
}

// Sync flushes any buffered log entries.
func Sync() error {
	if err := L().Sync(); err != nil {
		return err
	}
	if err := S().Sync(); err != nil {
		return err
	}
	var reterr error
	_globalLevelLogger.Range(func(key, val interface{}) bool {
		l := val.(*zap.Logger)
		if err := l.Sync(); err != nil {
			reterr = err
			return false
		}
		return true
	})
	return reterr
}

func Level() zap.AtomicLevel {
	return _globalP.Load().(*ZapProperties).Level
}
