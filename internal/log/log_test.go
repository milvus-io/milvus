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
	"io/ioutil"
	"os"
	"testing"
	"time"

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
	Sync()
	ts.assertMessagesContains("log_test.go:")

	ts = newTestLogSpy(t)
	logger, _, _ = InitTestLogger(ts, conf)
	ReplaceGlobals(logger, nil)

	newLogger := With(zap.String("name", "tester"), zap.Int64("age", 42))
	newLogger.Info("hello")
	newLogger.Debug("world")
	Sync()
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

func TestInvalidFileConfig(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("/tmp", "invalid-log-test")
	defer os.RemoveAll(tmpDir)

	invalidFileConf := FileLogConfig{
		Filename: tmpDir,
	}
	conf := &Config{Level: "debug", File: invalidFileConf, DisableTimestamp: true}

	_, _, err := InitLogger(conf)
	assert.Equal(t, "can't use directory as log file name", err.Error())
}

func TestLevelGetterAndSetter(t *testing.T) {
	conf := &Config{Level: "debug", File: FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := InitLogger(conf)
	ReplaceGlobals(logger, p)

	assert.Equal(t, zap.DebugLevel, GetLevel())

	SetLevel(zap.ErrorLevel)
	assert.Equal(t, zap.ErrorLevel, GetLevel())
}

func TestSampling(t *testing.T) {
	sample, drop := make(chan zapcore.SamplingDecision, 1), make(chan zapcore.SamplingDecision, 1)
	samplingConf := zap.SamplingConfig{
		Initial:    1,
		Thereafter: 2,
		Hook: func(entry zapcore.Entry, decision zapcore.SamplingDecision) {
			switch decision {
			case zapcore.LogSampled:
				sample <- decision
			case zapcore.LogDropped:
				drop <- decision
			}
		},
	}
	conf := &Config{Level: "debug", File: FileLogConfig{}, Sampling: &samplingConf}

	ts := newTestLogSpy(t)
	logger, p, _ := InitTestLogger(ts, conf)
	ReplaceGlobals(logger, p)

	for i := 0; i < 10; i++ {
		Debug("test")
		if i%2 == 0 {
			<-sample
		} else {
			<-drop
		}
	}
}

func TestRatedLog(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, p, _ := InitTestLogger(ts, conf)
	ReplaceGlobals(logger, p)

	time.Sleep(time.Duration(1) * time.Second)
	success := RatedDebug(1.0, "test")
	assert.True(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = RatedInfo(1.0, "test")
	assert.True(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = RatedWarn(1.0, "test")
	assert.True(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = RatedInfo(100.0, "test")
	assert.False(t, success)

	successNum := 0
	for i := 0; i < 1000; i++ {
		if RatedInfo(1.0, "test") {
			successNum++
		}
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	// due to the rate limit, not all
	assert.True(t, successNum < 1000)
	assert.True(t, successNum > 10)

	time.Sleep(time.Duration(3) * time.Second)
	success = RatedInfo(3.0, "test")
	assert.True(t, success)
	Sync()
}
