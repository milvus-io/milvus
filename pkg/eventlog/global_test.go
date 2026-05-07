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

package eventlog

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"
)

type GlobalLoggerSuite struct {
	suite.Suite
}

func (s *GlobalLoggerSuite) TearDownTest() {
	global.Store(nil)
}

func (s *GlobalLoggerSuite) TestGetGlobalLogger() {
	l := getGlobalLogger()
	s.NotNil(l)

	s.Equal(Level_Info, l.level)

	s.Equal(global.Load(), l)

	la := getGlobalLogger()
	s.Equal(l, la)
}

func (s *GlobalLoggerSuite) TestRecord() {
	calls := make(map[*testLogger]*testLoggerCalls)
	recordMock := mockey.Mock((*testLogger).Record).To(
		func(logger *testLogger, evt Evt) {
			calls[logger].records = append(calls[logger].records, evt)
		},
	).Build()
	defer recordMock.UnPatch()

	logger1 := newTestLogger(calls)
	logger2 := newTestLogger(calls)

	getGlobalLogger().Register("logger1", logger1)
	getGlobalLogger().Register("logger2", logger2)

	rawEvt := NewRawEvt(Level_Info, "test")

	getGlobalLogger().Record(rawEvt)

	s.Equal([]Evt{rawEvt}, calls[logger1].records)
	s.Equal([]Evt{rawEvt}, calls[logger2].records)

	logger3 := newTestLogger(calls)
	getGlobalLogger().Register("logger3", logger3)

	rawEvt = NewRawEvt(Level_Debug, "test")

	getGlobalLogger().Record(rawEvt)

	s.Equal(1, calls[logger1].recordCount())
	s.Equal(1, calls[logger2].recordCount())
	s.Equal(0, calls[logger3].recordCount())
}

func (s *GlobalLoggerSuite) TestRecordFunc() {
	calls := make(map[*testLogger]*testLoggerCalls)
	recordFuncMock := mockey.Mock((*testLogger).RecordFunc).To(
		func(logger *testLogger, level Level, fn func() Evt) {
			calls[logger].recordFuncs = append(calls[logger].recordFuncs, testRecordFuncCall{
				level: level,
				evt:   fn(),
			})
		},
	).Build()
	defer recordFuncMock.UnPatch()

	logger1 := newTestLogger(calls)
	logger2 := newTestLogger(calls)

	getGlobalLogger().Register("logger1", logger1)
	getGlobalLogger().Register("logger2", logger2)

	rawEvt := NewRawEvt(Level_Info, "test")

	getGlobalLogger().RecordFunc(Level_Info, func() Evt { return rawEvt })

	s.Equal([]testRecordFuncCall{{level: Level_Info, evt: rawEvt}}, calls[logger1].recordFuncs)
	s.Equal([]testRecordFuncCall{{level: Level_Info, evt: rawEvt}}, calls[logger2].recordFuncs)

	logger3 := newTestLogger(calls)
	getGlobalLogger().Register("logger3", logger3)

	rawEvt = NewRawEvt(Level_Debug, "test")

	getGlobalLogger().RecordFunc(Level_Debug, func() Evt { return rawEvt })

	s.Equal(1, calls[logger1].recordFuncCount())
	s.Equal(1, calls[logger2].recordFuncCount())
	s.Equal(0, calls[logger3].recordFuncCount())
}

func (s *GlobalLoggerSuite) TestFlush() {
	calls := make(map[*testLogger]*testLoggerCalls)
	flushMock := mockey.Mock((*testLogger).Flush).To(
		func(logger *testLogger) error {
			calls[logger].flushes++
			return nil
		},
	).Build()
	defer flushMock.UnPatch()

	logger1 := newTestLogger(calls)
	logger2 := newTestLogger(calls)

	getGlobalLogger().Register("logger1", logger1)
	getGlobalLogger().Register("logger2", logger2)

	err := getGlobalLogger().Flush()
	s.NoError(err)
	s.Equal(1, calls[logger1].flushes)
	s.Equal(1, calls[logger2].flushes)
}

func TestGlobalLogger(t *testing.T) {
	suite.Run(t, new(GlobalLoggerSuite))
}
