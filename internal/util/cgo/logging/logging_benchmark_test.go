package logging

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func BenchmarkGlog(b *testing.B) {
	InitGoogleLogging()
	logs := generateTestLogs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeLogsWithConcurrency(logs)
	}
}

func BenchmarkZapLog(b *testing.B) {
	InitGoogleLoggingWithZapSink()
	logs := generateTestLogs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeLogsWithConcurrency(logs)
	}
}

var initOnce sync.Once

func BenchmarkZapAsyncLog4KB(b *testing.B) {
	benchmarkZapAsyncLog(b, 4*1024)
}

func BenchmarkZapAsyncLog4MB(b *testing.B) {
	benchmarkZapAsyncLog(b, 4*1024*1024)
}

func benchmarkZapAsyncLog(b *testing.B, bufferSize int) {
	initOnce.Do(func() {
		params := paramtable.Get()
		logConfig := log.Config{
			Level:     params.LogCfg.Level.GetValue(),
			GrpcLevel: params.LogCfg.GrpcLogLevel.GetValue(),
			Format:    params.LogCfg.Format.GetValue(),
			Stdout:    params.LogCfg.Stdout.GetAsBool(),
			File: log.FileLogConfig{
				RootPath:   params.LogCfg.RootPath.GetValue(),
				MaxSize:    params.LogCfg.MaxSize.GetAsInt(),
				MaxDays:    params.LogCfg.MaxAge.GetAsInt(),
				MaxBackups: params.LogCfg.MaxBackups.GetAsInt(),
			},
			AsyncWriteEnable:         params.LogCfg.AsyncWriteEnable.GetAsBool(),
			AsyncWriteFlushInterval:  params.LogCfg.AsyncWriteFlushInterval.GetAsDurationByParse(),
			AsyncWriteDroppedTimeout: params.LogCfg.AsyncWriteDroppedTimeout.GetAsDurationByParse(),
			AsyncWriteStopTimeout:    params.LogCfg.AsyncWriteStopTimeout.GetAsDurationByParse(),
			AsyncWritePendingLength:  params.LogCfg.AsyncWritePendingLength.GetAsInt(),
			AsyncWriteBufferSize:     bufferSize,
			AsyncWriteMaxBytesPerLog: int(params.LogCfg.AsyncWriteMaxBytesPerLog.GetAsSize()),
		}
		logutil.SetupLogger(&logConfig)
	})

	InitGoogleLoggingWithZapSink()
	logs := generateTestLogs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeLogsWithConcurrency(logs)
	}
}

func generateTestLogs() []string {
	seed := int64(123456)
	r := rand.New(rand.NewSource(seed))

	logs := make([]string, 1000)
	for i := 0; i < len(logs); i++ {
		n := r.Intn(4*1024) + 10
		logs[i] = funcutil.RandomString(n)
	}
	return logs
}

func writeLogsWithConcurrency(logs []string) {
	currency := 24
	ch := make(chan string, len(logs))
	for _, log := range logs {
		ch <- log
	}
	close(ch)
	wg := sync.WaitGroup{}
	writeLogs := func(logs chan string) {
		defer wg.Done()
		for log := range logs {
			GoogleLoggingAtLevel(GlogInfo, log)
		}
	}
	wg.Add(currency)
	for i := 0; i < currency; i++ {
		go writeLogs(ch)
	}
	wg.Wait()
}
