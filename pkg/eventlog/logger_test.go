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

type LoggerSuite struct {
	suite.Suite
}

func (s *LoggerSuite) TearDownTest() {
	global.Store(nil)
}

func (s *LoggerSuite) TestRecord() {
	calls := make(map[*struct{ Logger }]*loggerCalls)
	recordMock := mockey.Mock((*struct{ Logger }).Record).To(
		func(logger *struct{ Logger }, evt Evt) {
			calls[logger].records = append(calls[logger].records, evt)
		},
	).Build()
	defer recordMock.UnPatch()

	logger1 := newTestLogger(calls)
	logger2 := newTestLogger(calls)
	Register("logger1", logger1)
	Register("logger2", logger2)

	rawEvt := NewRawEvt(Level_Info, "test")

	Record(rawEvt)

	s.Equal([]Evt{rawEvt}, calls[logger1].records)
	s.Equal([]Evt{rawEvt}, calls[logger2].records)

	logger3 := newTestLogger(calls)
	Register("logger3", logger3)

	rawEvt = NewRawEvt(Level_Debug, "test")

	Record(rawEvt)

	s.Equal(1, calls[logger1].recordCount())
	s.Equal(1, calls[logger2].recordCount())
	s.Equal(0, calls[logger3].recordCount())
}

func (s *LoggerSuite) TestRecordFunc() {
	calls := make(map[*struct{ Logger }]*loggerCalls)
	recordFuncMock := mockey.Mock((*struct{ Logger }).RecordFunc).To(
		func(logger *struct{ Logger }, level Level, fn func() Evt) {
			calls[logger].recordFuncs = append(calls[logger].recordFuncs, recordFuncCall{
				level: level,
				evt:   fn(),
			})
		},
	).Build()
	defer recordFuncMock.UnPatch()

	logger1 := newTestLogger(calls)
	logger2 := newTestLogger(calls)

	Register("logger1", logger1)
	Register("logger2", logger2)

	rawEvt := NewRawEvt(Level_Info, "test")

	RecordFunc(Level_Info, func() Evt { return rawEvt })

	s.Equal([]recordFuncCall{{level: Level_Info, evt: rawEvt}}, calls[logger1].recordFuncs)
	s.Equal([]recordFuncCall{{level: Level_Info, evt: rawEvt}}, calls[logger2].recordFuncs)

	logger3 := newTestLogger(calls)
	Register("logger3", logger3)

	rawEvt = NewRawEvt(Level_Debug, "test")

	RecordFunc(Level_Debug, func() Evt { return rawEvt })

	s.Equal(1, calls[logger1].recordFuncCount())
	s.Equal(1, calls[logger2].recordFuncCount())
	s.Equal(0, calls[logger3].recordFuncCount())
}

func (s *LoggerSuite) TestFlush() {
	calls := make(map[*struct{ Logger }]*loggerCalls)
	flushMock := mockey.Mock((*struct{ Logger }).Flush).To(
		func(logger *struct{ Logger }) error {
			calls[logger].flushes++
			return nil
		},
	).Build()
	defer flushMock.UnPatch()

	logger1 := newTestLogger(calls)
	logger2 := newTestLogger(calls)
	Register("logger1", logger1)
	Register("logger2", logger2)

	err := Flush()
	s.NoError(err)
	s.Equal(1, calls[logger1].flushes)
	s.Equal(1, calls[logger2].flushes)
}

func TestLogger(t *testing.T) {
	suite.Run(t, new(LoggerSuite))
}

type recordFuncCall struct {
	level Level
	evt   Evt
}

type loggerCalls struct {
	records     []Evt
	recordFuncs []recordFuncCall
	flushes     int
}

func (c *loggerCalls) recordCount() int {
	return len(c.records)
}

func (c *loggerCalls) recordFuncCount() int {
	return len(c.recordFuncs)
}

func newTestLogger(calls map[*struct{ Logger }]*loggerCalls) *struct{ Logger } {
	logger := &struct{ Logger }{}
	calls[logger] = &loggerCalls{}
	return logger
}
