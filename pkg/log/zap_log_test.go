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
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type username string

func (n username) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("username", string(n))
	return nil
}

func TestLog(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, _, _ := InitTestLogger(ts, conf)
	sugar := logger.Sugar()
	defer func() {
		_ = sugar.Sync()
	}()

	sugar.Infow("failed to fetch URL",
		"url", "http://example.com",
		"attempt", 3,
		"backoff", time.Second,
	)
	sugar.Infof(`failed to "fetch" [URL]: %s`, "http://example.com")
	sugar.Debugw("Slow query",
		"sql", `SELECT * FROM TABLE
	WHERE ID="abc"`,
		"duration", 1300*time.Millisecond,
		"process keys", 1500,
	)
	sugar.Info("Welcome")
	sugar.Info("欢迎")
	sugar.Warnw("Type",
		"Counter", math.NaN(),
		"Score", math.Inf(1),
	)

	logger.With(zap.String("connID", "1"), zap.String("traceID", "dse1121")).Info("new connection")
	logger.Info("Testing typs",
		zap.String("filed1", "noquote"),
		zap.String("filed2", "in quote"),
		zap.Strings("urls", []string{"http://mock1.com:2347", "http://mock2.com:2432"}),
		zap.Strings("urls-peer", []string{"t1", "t2 fine"}),
		zap.Uint64s("store ids", []uint64{1, 4, 5}),
		zap.Object("object", username("user1")),
		zap.Object("object2", username("user 2")),
		zap.Binary("binary", []byte("ab123")),
		zap.Bool("is processed", true),
		zap.ByteString("bytestring", []byte("noquote")),
		zap.ByteString("bytestring", []byte("in quote")),
		zap.Int8("int8", int8(1)),
		zap.Uintptr("ptr", 0xa),
		zap.Reflect("reflect", []int{1, 2}),
		zap.Stringer("stringer", net.ParseIP("127.0.0.1")),
		zap.Bools("array bools", []bool{true}),
		zap.Bools("array bools", []bool{true, true, false}),
		zap.Complex128("complex128", 1+2i),
		zap.Strings("test", []string{
			"💖",
			"�",
			"☺☻☹",
			"日a本b語ç日ð本Ê語þ日¥本¼語i日©",
			"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©",
			"\x80\x80\x80\x80",
			"<car><mirror>XML</mirror></car>",
		}),
		zap.Duration("duration", 10*time.Second),
	)
	ts.assertMessages(
		`[INFO] [log/zap_log_test.go:65] ["failed to fetch URL"] [url=http://example.com] [attempt=3] [backoff=1s]`,
		`[INFO] [log/zap_log_test.go:70] ["failed to \"fetch\" [URL]: http://example.com"]`,
		`[DEBUG] [log/zap_log_test.go:71] ["Slow query"] [sql="SELECT * FROM TABLE\n\tWHERE ID=\"abc\""] [duration=1.3s] ["process keys"=1500]`,
		`[INFO] [log/zap_log_test.go:77] [Welcome]`,
		`[INFO] [log/zap_log_test.go:78] [欢迎]`,
		`[WARN] [log/zap_log_test.go:79] [Type] [Counter=NaN] [Score=+Inf]`,
		`[INFO] [log/zap_log_test.go:84] ["new connection"] [connID=1] [traceID=dse1121]`,
		`[INFO] [log/zap_log_test.go:85] ["Testing typs"] [filed1=noquote] `+
			`[filed2="in quote"] [urls="[http://mock1.com:2347,http://mock2.com:2432]"] `+
			`[urls-peer="[t1,\"t2 fine\"]"] ["store ids"="[1,4,5]"] [object="{username=user1}"] `+
			`[object2="{username=\"user 2\"}"] [binary="YWIxMjM="] ["is processed"=true] `+
			`[bytestring=noquote] [bytestring="in quote"] [int8=1] [ptr=10] [reflect="[1,2]"] [stringer=127.0.0.1] `+
			`["array bools"="[true]"] ["array bools"="[true,true,false]"] [complex128=1+2i] `+
			`[test="[💖,�,☺☻☹,日a本b語ç日ð本Ê語þ日¥本¼語i日©,日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©,\\ufffd\\ufffd\\ufffd\\ufffd,`+
			`<car><mirror>XML</mirror></car>]"] [duration=10s]`,
	)

	assert.PanicsWithValue(t, "unknown", func() { sugar.Panic("unknown") })
}

func TestTimeEncoder(t *testing.T) {
	sec := int64(1547192741)
	nsec := int64(165279177)
	as, err := time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)

	tt := time.Unix(sec, nsec).In(as)
	conf := &Config{Level: "debug", File: FileLogConfig{}, DisableTimestamp: true}
	enc := NewTextEncoderByConfig(conf).(*textEncoder)
	DefaultTimeEncoder(tt, enc)
	assert.Equal(t, "2019/01/11 15:45:41.165 +08:00", enc.buf.String())

	enc.buf.Reset()
	utc, err := time.LoadLocation("UTC")
	assert.NoError(t, err)

	utcTime := tt.In(utc)
	DefaultTimeEncoder(utcTime, enc)
	assert.Equal(t, "2019/01/11 07:45:41.165 +00:00", enc.buf.String())
}

// See [logger-header]https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-header-section.
func TestZapCaller(t *testing.T) {
	data := []zapcore.EntryCaller{
		/* #nosec G103 */ {Defined: true, PC: uintptr(unsafe.Pointer(nil)), File: "server.go", Line: 132},
		/* #nosec G103 */ {Defined: true, PC: uintptr(unsafe.Pointer(nil)), File: "server/coordinator.go", Line: 20},
		/* #nosec G103 */ {Defined: true, PC: uintptr(unsafe.Pointer(nil)), File: `z\test_coordinator1.go`, Line: 20},
		/* #nosec G103 */ {Defined: false, PC: uintptr(unsafe.Pointer(nil)), File: "", Line: 0},
	}
	expect := []string{
		"server.go:132",
		"server/coordinator.go:20",
		"\"z\\\\test_coordinator1.go:20\"",
		"undefined",
	}
	conf := &Config{Level: "deug", File: FileLogConfig{}, DisableTimestamp: true}
	enc := NewTextEncoderByConfig(conf).(*textEncoder)

	for i, d := range data {
		ShortCallerEncoder(d, enc)
		assert.Equal(t, expect[i], enc.buf.String())
		enc.buf.Reset()
	}
}

func TestLogJSON(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true, Format: "json"}
	logger, _, _ := InitTestLogger(ts, conf, zap.AddStacktrace(zapcore.FatalLevel))
	sugar := logger.Sugar()
	defer sugar.Sync()

	sugar.Infow("failed to fetch URL",
		"url", "http://example.com",
		"attempt", 3,
		"backoff", time.Second,
	)
	logger.With(zap.String("connID", "1"), zap.String("traceID", "dse1121")).Info("new connection")
	ts.assertMessages("{\"level\":\"INFO\",\"caller\":\"log/zap_log_test.go:188\",\"message\":\"failed to fetch URL\",\"url\":\"http://example.com\",\"attempt\":3,\"backoff\":\"1s\"}",
		"{\"level\":\"INFO\",\"caller\":\"log/zap_log_test.go:193\",\"message\":\"new connection\",\"connID\":\"1\",\"traceID\":\"dse1121\"}")
}

func TestRotateLog(t *testing.T) {
	cases := []struct {
		desc            string
		maxSize         int
		writeSize       int
		expectedFileNum int
	}{
		{"test limited max size", 1, 1 * 1024 * 1024, 2},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			tempDir := t.TempDir()
			conf := &Config{
				Level: "info",
				File: FileLogConfig{
					Filename: tempDir + "/test.log",
					MaxSize:  c.maxSize,
				},
			}
			logger, _, err := InitLogger(conf)
			assert.NoError(t, err)

			var data []byte
			for i := 1; i <= c.writeSize; i++ {
				if i%1000 != 0 {
					data = append(data, 'd')
					continue
				}
				logger.Info(string(data))
				data = data[:0]
			}
			files, _ := os.ReadDir(tempDir)
			assert.Len(t, files, c.expectedFileNum)
		})
	}
}

func TestWithOptions(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{
		Level:               "debug",
		DisableTimestamp:    true,
		DisableErrorVerbose: true,
	}
	logger, _, _ := InitTestLogger(ts, conf, zap.AddStacktrace(zapcore.FatalLevel))
	logger.Error("Testing", zap.Error(errors.New("log-with-option")))
	ts.assertMessagesNotContains("errorVerbose")
	ts.assertMessagesNotContains("stack")
}

func TestNamedLogger(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{
		Level:               "debug",
		DisableTimestamp:    true,
		DisableErrorVerbose: true,
	}
	logger, _, _ := InitTestLogger(ts, conf, zap.AddStacktrace(zapcore.FatalLevel))
	namedLogger := logger.Named("testLogger")
	namedLogger.Error("testing")
	ts.assertMessagesContains("testLogger")
}

func TestErrorLog(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, _, _ := InitTestLogger(ts, conf)
	logger.Error("", zap.NamedError("err", errors.New("log-stack-test")))
	ts.assertMessagesContains("[err=log-stack-test]")
	ts.assertMessagesContains("] [errVerbose=\"")
}

// testLogSpy is a testing.TB that captures logged messages.
type testLogSpy struct {
	testing.TB

	failed   bool
	Messages []string
}

func newTestLogSpy(t testing.TB) *testLogSpy {
	return &testLogSpy{TB: t}
}

func (t *testLogSpy) Fail() {
	t.failed = true
}

func (t *testLogSpy) Failed() bool {
	return t.failed
}

func (t *testLogSpy) FailNow() {
	t.Fail()
	t.TB.FailNow()
}

func (t *testLogSpy) CleanBuffer() {
	t.Messages = []string{}
}

func (t *testLogSpy) Logf(format string, args ...interface{}) {
	// Log messages are in the format,
	//
	//   2017-10-27T13:03:01.000-0700	DEBUG	your message here	{data here}
	//
	// We strip the first part of these messages because we can't really test
	// for the timestamp from these tests.
	m := fmt.Sprintf(format, args...)
	m = m[strings.IndexByte(m, '\t')+1:]
	t.Messages = append(t.Messages, m)
	t.TB.Log(m)
}

func (t *testLogSpy) assertMessages(msgs ...string) {
	assert.Equal(t.TB, msgs, t.Messages)
}

func (t *testLogSpy) assertMessagesContains(msg string) {
	for _, actualMsg := range t.Messages {
		assert.Contains(t.TB, actualMsg, msg)
	}
}

func (t *testLogSpy) assertMessagesNotContains(msg string) {
	for _, actualMsg := range t.Messages {
		assert.NotContains(t.TB, actualMsg, msg)
	}
}

func (t *testLogSpy) assertLastMessageContains(msg string) {
	if len(t.Messages) == 0 {
		assert.Error(t.TB, errors.New("empty message"))
	}
	assert.Contains(t.TB, t.Messages[len(t.Messages)-1], msg)
}

func (t *testLogSpy) assertLastMessageNotContains(msg string) {
	if len(t.Messages) == 0 {
		assert.Error(t.TB, errors.New("empty message"))
	}
	assert.NotContains(t.TB, t.Messages[len(t.Messages)-1], msg)
}

func (t *testLogSpy) assertMessageContainAny(msg string) {
	found := false
	for _, actualMsg := range t.Messages {
		if strings.Contains(actualMsg, msg) {
			found = true
		}
	}
	assert.True(t, found, "can't found any message contain `%s`, all messages: %v", msg, fmtMsgs(t.Messages))
}

func fmtMsgs(messages []string) string {
	builder := strings.Builder{}
	builder.WriteString("[")
	for i, msg := range messages {
		if i == len(messages)-1 {
			builder.WriteString(fmt.Sprintf("`%s]`", msg))
		} else {
			builder.WriteString(fmt.Sprintf("`%s`, ", msg))
		}
	}
	return builder.String()
}
