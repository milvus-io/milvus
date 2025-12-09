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
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLogMaxSize = 300 // MB
)

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	// Log rootpath
	RootPath string `toml:"rootpath" json:"rootpath"`
	// Log filename, leave empty to disable file log.
	Filename string `toml:"filename" json:"filename"`
	// Max size for a single file, in MB.
	MaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	MaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups" json:"max-backups"`
}

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// grpc log level
	GrpcLevel string `toml:"grpc-level" json:"grpc-level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// Stdout enable or not.
	Stdout bool `toml:"stdout" json:"stdout"`
	// File log config.
	File FileLogConfig `toml:"file" json:"file"`
	// Development puts the logger in development mode, which changes the
	// behavior of DPanicLevel and takes stacktraces more liberally.
	Development bool `toml:"development" json:"development"`
	// DisableCaller stops annotating logs with the calling function's file
	// name and line number. By default, all logs are annotated.
	DisableCaller bool `toml:"disable-caller" json:"disable-caller"`
	// DisableStacktrace completely disables automatic stacktrace capturing. By
	// default, stacktraces are captured for WarnLevel and above logs in
	// development and ErrorLevel and above in production.
	DisableStacktrace bool `toml:"disable-stacktrace" json:"disable-stacktrace"`
	// DisableErrorVerbose stops annotating logs with the full verbose error
	// message.
	DisableErrorVerbose bool `toml:"disable-error-verbose" json:"disable-error-verbose"`
	// SamplingConfig sets a sampling strategy for the logger. Sampling caps the
	// global CPU and I/O load that logging puts on your process while attempting
	// to preserve a representative subset of your logs.
	//
	// Values configured here are per-second. See zapcore.NewSampler for details.
	Sampling *zap.SamplingConfig `toml:"sampling" json:"sampling"`

	// AsyncWriteEnable enables async write for the logger.
	AsyncWriteEnable bool `toml:"async-write-enable" json:"async-write-enable"`

	// AsyncWriteFlushInterval is the interval to flush the logs
	AsyncWriteFlushInterval time.Duration `toml:"async-write-flush-interval" json:"async-write-flush-interval"`

	// AsyncWriteDroppedTimeout is the timeout to drop the write request if the buffer is full
	AsyncWriteDroppedTimeout time.Duration `toml:"async-write-dropped-timeout" json:"async-write-dropped-timeout"`

	// AsyncWriteNonDroppableLevel is the level that will not be dropped when the buffer is full
	AsyncWriteNonDroppableLevel string `toml:"async-write-non-droppable-level" json:"async-write-non-droppable-level"`

	// AsyncWriteStopTimeout is the timeout to stop the async write
	AsyncWriteStopTimeout time.Duration `toml:"async-write-stop-timeout" json:"async-write-stop-timeout"`

	// AsyncWritePendingLength is the maximum number of pending write requests, the exceeded log operation will be dropped
	AsyncWritePendingLength int `toml:"async-write-pending-length" json:"async-write-pending-length"`

	// AsyncWriteBufferSize is the size of the write buffer
	AsyncWriteBufferSize int `toml:"async-write-buffer-size" json:"async-write-buffer-size"`

	// AsyncWriteMaxBytesPerLog is the max bytes per log
	AsyncWriteMaxBytesPerLog int `toml:"async-write-max-bytes-per-log" json:"async-write-max-bytes-per-log"`
}

// ZapProperties records some information about zap.
type ZapProperties struct {
	Core   zapcore.Core
	Syncer zapcore.WriteSyncer
	Level  zap.AtomicLevel
}

func newZapTextEncoder(cfg *Config) zapcore.Encoder {
	return NewTextEncoderByConfig(cfg)
}

func (cfg *Config) buildOptions(errSink zapcore.WriteSyncer) []zap.Option {
	opts := []zap.Option{zap.ErrorOutput(errSink)}

	if cfg.Development {
		opts = append(opts, zap.Development())
	}

	if !cfg.DisableCaller {
		opts = append(opts, zap.AddCaller())
	}

	stackLevel := zap.ErrorLevel
	if cfg.Development {
		stackLevel = zap.WarnLevel
	}
	if !cfg.DisableStacktrace {
		opts = append(opts, zap.AddStacktrace(stackLevel))
	}

	if cfg.Sampling != nil {
		opts = append(opts, zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(core, time.Second, cfg.Sampling.Initial, cfg.Sampling.Thereafter, zapcore.SamplerHook(cfg.Sampling.Hook))
		}))
	}
	return opts
}

// initialize initializes the config.
func (cfg *Config) initialize() {
	if cfg.AsyncWriteFlushInterval <= 0 {
		cfg.AsyncWriteFlushInterval = 10 * time.Second
	}
	if cfg.AsyncWriteDroppedTimeout <= 0 {
		cfg.AsyncWriteDroppedTimeout = 100 * time.Millisecond
	}
	if _, err := zapcore.ParseLevel(cfg.AsyncWriteNonDroppableLevel); cfg.AsyncWriteNonDroppableLevel == "" || err != nil {
		cfg.AsyncWriteNonDroppableLevel = zapcore.ErrorLevel.String()
	}
	if cfg.AsyncWriteStopTimeout <= 0 {
		cfg.AsyncWriteStopTimeout = 1 * time.Second
	}
	if cfg.AsyncWritePendingLength <= 0 {
		cfg.AsyncWritePendingLength = 1024
	}
	if cfg.AsyncWriteBufferSize <= 0 {
		cfg.AsyncWriteBufferSize = 4 * 1024
	}
	if cfg.AsyncWriteMaxBytesPerLog <= 0 {
		cfg.AsyncWriteMaxBytesPerLog = 1024 * 1024
	}
}
