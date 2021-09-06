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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestExport(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, _, _ := InitTestLogger(ts, conf)
	ReplaceGlobals(logger, nil)

	Info("Testing")
	Debug("Testing")
	Warn("Testing")
	Error("Testing")
	ts.assertMessagesContains("log_test.go:")

	ts = newTestLogSpy(t)
	logger, _, _ = InitTestLogger(ts, conf)
	ReplaceGlobals(logger, nil)

	newLogger := With(zap.String("name", "tester"), zap.Int64("age", 42))
	newLogger.Info("hello")
	newLogger.Debug("world")
	ts.assertMessagesContains(`name=tester`)
	ts.assertMessagesContains(`age=42`)
}

func TestZapTextEncoder(t *testing.T) {
	conf := &Config{Level: "debug", File: FileLogConfig{}, DisableTimestamp: true}

	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	encoder := NewTextEncoder(conf)
	logger := zap.New(zapcore.NewCore(encoder, zapcore.AddSync(writer), zapcore.InfoLevel)).Sugar()

	logger.Info("this is a message from zap")
	_ = writer.Flush()
	assert.Equal(t, `[INFO] ["this is a message from zap"]`+"\n", buffer.String())
}
