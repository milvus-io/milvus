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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestExport(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, _, _ := InitTestLogger(ts, conf, zap.AddCallerSkip(1))
	ReplaceGlobals(logger, nil)

	Info("Testing")
	Debug("Testing")
	Warn("Testing")
	Error("Testing")
	Sync()
	ts.assertMessagesContains("log_test.go:")
	logPanic()

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

func logPanic() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("logPanic recover")
		}
	}()
	Panic("Testing")
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
	tmpDir := t.TempDir()

	invalidFileConf := FileLogConfig{
		Filename: tmpDir,
	}
	conf := &Config{Level: "debug", File: invalidFileConf, DisableTimestamp: true}

	_, _, err := InitLogger(conf)
	assert.Equal(t, "can't use directory as log file name", err.Error())

	// invalid level
	conf = &Config{Level: "debuge", DisableTimestamp: true}
	_, _, err = InitLogger(conf)
	assert.Error(t, err)
}

func TestLevelGetterAndSetter(t *testing.T) {
	conf := &Config{Level: "debug", File: FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := InitLogger(conf)
	ReplaceGlobals(logger, p)

	assert.Equal(t, zap.DebugLevel, GetLevel())

	SetLevel(zap.ErrorLevel)
	assert.Equal(t, zap.ErrorLevel, GetLevel())
}

func TestUpdateLogLevelThroughHttp(t *testing.T) {
	httpServer := &http.Server{Addr: ":9081"}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			Fatal(err.Error())
		}
	}()

	SetLevel(zap.DebugLevel)
	assert.Equal(t, zap.DebugLevel, GetLevel())

	// replace global logger, log change will not be affected.
	conf := &Config{Level: "info", File: FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := InitLogger(conf)
	ReplaceGlobals(logger, p)
	assert.Equal(t, zap.InfoLevel, GetLevel())

	// change log level through http
	payload, err := json.Marshal(map[string]interface{}{"level": "error"})
	if err != nil {
		Fatal(err.Error())
	}

	req, err := http.NewRequest(http.MethodPut, "http://localhost:9081/log/level", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		Fatal(err.Error())
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		Fatal(err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		Fatal(err.Error())
	}
	assert.Equal(t, "{\"level\":\"error\"}\n", string(body))
	assert.Equal(t, zap.ErrorLevel, GetLevel())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		Fatal(err.Error())
	}
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

func TestLeveledLogger(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true, DisableCaller: true}
	logger, _, _ := InitTestLogger(ts, conf)
	replaceLeveledLoggers(logger)

	debugL().Debug("DEBUG LOG")
	debugL().Info("INFO LOG")
	debugL().Warn("WARN LOG")
	debugL().Error("ERROR LOG")
	Sync()

	ts.assertMessageContainAny(`[DEBUG] ["DEBUG LOG"]`)
	ts.assertMessageContainAny(`[INFO] ["INFO LOG"]`)
	ts.assertMessageContainAny(`[WARN] ["WARN LOG"]`)
	ts.assertMessageContainAny(`[ERROR] ["ERROR LOG"]`)

	ts.CleanBuffer()

	infoL().Debug("DEBUG LOG")
	infoL().Info("INFO LOG")
	infoL().Warn("WARN LOG")
	infoL().Error("ERROR LOG")
	Sync()

	ts.assertMessagesNotContains(`[DEBUG] ["DEBUG LOG"]`)
	ts.assertMessageContainAny(`[INFO] ["INFO LOG"]`)
	ts.assertMessageContainAny(`[WARN] ["WARN LOG"]`)
	ts.assertMessageContainAny(`[ERROR] ["ERROR LOG"]`)
	ts.CleanBuffer()

	warnL().Debug("DEBUG LOG")
	warnL().Info("INFO LOG")
	warnL().Warn("WARN LOG")
	warnL().Error("ERROR LOG")
	Sync()

	ts.assertMessagesNotContains(`[DEBUG] ["DEBUG LOG"]`)
	ts.assertMessagesNotContains(`[INFO] ["INFO LOG"]`)
	ts.assertMessageContainAny(`[WARN] ["WARN LOG"]`)
	ts.assertMessageContainAny(`[ERROR] ["ERROR LOG"]`)
	ts.CleanBuffer()

	errorL().Debug("DEBUG LOG")
	errorL().Info("INFO LOG")
	errorL().Warn("WARN LOG")
	errorL().Error("ERROR LOG")
	Sync()

	ts.assertMessagesNotContains(`[DEBUG] ["DEBUG LOG"]`)
	ts.assertMessagesNotContains(`[INFO] ["INFO LOG"]`)
	ts.assertMessagesNotContains(`[WARN] ["WARN LOG"]`)
	ts.assertMessageContainAny(`[ERROR] ["ERROR LOG"]`)

	ts.CleanBuffer()

	ctx := withLogLevel(context.TODO(), zapcore.DPanicLevel)
	assert.Equal(t, Ctx(ctx).Logger, L())

	// set invalid level
	orgLevel := GetLevel()
	SetLevel(zapcore.FatalLevel + 1)
	assert.Equal(t, ctxL(), L())
	SetLevel(orgLevel)

}
