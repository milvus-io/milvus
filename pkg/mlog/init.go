package mlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"gopkg.in/natefinch/lumberjack.v2"
)

var globalP, globalS, globalCleanup atomic.Value

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
	level := zapcore.DebugLevel
	parsedLevel := cfg.Level
	if strings.EqualFold(parsedLevel, "trace") {
		parsedLevel = "debug"
	}
	if err := level.UnmarshalText([]byte(parsedLevel)); err != nil {
		return nil, nil, err
	}
	r.Level.SetLevel(level)
	return debugL.WithOptions(zap.AddCallerSkip(1)), r, nil
}

// InitTestLogger initializes a logger for unit tests.
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

// InitLoggerWithWriteSyncer initializes a zap logger with the specified write syncer.
func InitLoggerWithWriteSyncer(cfg *Config, output zapcore.WriteSyncer, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	cfg.initialize()
	level := zap.NewAtomicLevel()
	parsedLevel := cfg.Level
	if strings.EqualFold(parsedLevel, "trace") {
		parsedLevel = "debug"
	}
	err := level.UnmarshalText([]byte(parsedLevel))
	if err != nil {
		return nil, nil, fmt.Errorf("initLoggerWithWriteSyncer UnmarshalText cfg.Level err:%w", err)
	}
	var core zapcore.Core
	if cfg.AsyncWriteEnable {
		asyncCore := NewAsyncTextIOCore(cfg, output, level)
		core = asyncCore
		registerCleanup(asyncCore.Stop)
	} else {
		core = NewTextCore(newZapTextEncoder(cfg), output, level)
	}
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

// L returns the global zap logger.
func L() *zap.Logger {
	return getLogger()
}

// S returns the global sugared logger.
func S() *zap.SugaredLogger {
	return globalS.Load().(*zap.SugaredLogger)
}

// Cleanup cleans up the global logger.
func Cleanup() {
	cleanup := globalCleanup.Load()
	if cleanup != nil {
		cleanup.(func())()
	}
}

// ReplaceGlobals replaces the global zap logger and associated properties.
func ReplaceGlobals(logger *zap.Logger, props *ZapProperties) {
	globalLogger.Store(logger)
	globalS.Store(logger.Sugar())
	if props != nil {
		globalP.Store(props)
		globalLevel.Store(&props.Level)
	}
}

func registerCleanup(cleanup func()) {
	oldCleanup := globalCleanup.Swap(cleanup)
	if oldCleanup != nil {
		oldCleanup.(func())()
	}
}
