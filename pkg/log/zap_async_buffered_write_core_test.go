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

package log

import (
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestAsyncBufferedWriteSyncer(t *testing.T) {
	blockWriter := &blockWriter{
		Writer: os.Stdout,
	}
	syncer := NewAsyncTextIOCore(
		&Config{
			AsyncWriteEnable:         true,
			AsyncWriteFlushInterval:  1 * time.Second,
			AsyncWriteDroppedTimeout: 100 * time.Millisecond,
			AsyncWriteStopTimeout:    1 * time.Second,
			AsyncWritePendingLength:  100,
			AsyncWriteBufferSize:     5,
			AsyncWriteMaxBytesPerLog: 2,
		},
		zapcore.AddSync(blockWriter),
		zap.DebugLevel,
	)
	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			syncer.Write(zapcore.Entry{
				Level:   zap.DebugLevel,
				Message: "test",
			}, []zapcore.Field{
				zap.String("test", "test"),
				zap.Int("test", 1),
			})
			wg.Done()
		}()
	}
	syncer.Sync()
	wg.Wait()
	syncer.Stop()
}

type blockWriter struct {
	io.Writer
}

func (s *blockWriter) Write(p []byte) (n int, err error) {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	if rand.Intn(10) == 0 {
		return 0, errors.New("write error")
	}
	return s.Writer.Write(p)
}

func (s *blockWriter) Sync() error {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	if rand.Intn(10) == 0 {
		return errors.New("sync error")
	}
	return nil
}
