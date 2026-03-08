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

	mock "github.com/stretchr/testify/mock"
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
	mock1 := NewMockLogger(s.T())
	mock2 := NewMockLogger(s.T())

	getGlobalLogger().Register("mock1", mock1)
	getGlobalLogger().Register("mock2", mock2)

	rawEvt := NewRawEvt(Level_Info, "test")

	mock1.EXPECT().Record(rawEvt)
	mock2.EXPECT().Record(rawEvt)

	getGlobalLogger().Record(rawEvt)

	mock3 := NewMockLogger(s.T())

	getGlobalLogger().Register("mock3", mock3) // register logger without expectations

	rawEvt = NewRawEvt(Level_Debug, "test")

	getGlobalLogger().Record(rawEvt)
}

func (s *GlobalLoggerSuite) TestRecordFunc() {
	mock1 := NewMockLogger(s.T())
	mock2 := NewMockLogger(s.T())

	getGlobalLogger().Register("mock1", mock1)
	getGlobalLogger().Register("mock2", mock2)

	rawEvt := NewRawEvt(Level_Info, "test")

	mock1.EXPECT().RecordFunc(mock.Anything, mock.Anything)
	mock2.EXPECT().RecordFunc(mock.Anything, mock.Anything)

	getGlobalLogger().RecordFunc(Level_Info, func() Evt { return rawEvt })

	mock3 := NewMockLogger(s.T())

	getGlobalLogger().Register("mock3", mock3) // register logger without expectations

	rawEvt = NewRawEvt(Level_Debug, "test")

	getGlobalLogger().RecordFunc(Level_Debug, func() Evt { return rawEvt })
}

func (s *GlobalLoggerSuite) TestFlush() {
	mock1 := NewMockLogger(s.T())
	mock2 := NewMockLogger(s.T())

	getGlobalLogger().Register("mock1", mock1)
	getGlobalLogger().Register("mock2", mock2)

	mock1.EXPECT().Flush().Return(nil)
	mock2.EXPECT().Flush().Return(nil)

	err := getGlobalLogger().Flush()
	s.NoError(err)
}

func TestGlobalLogger(t *testing.T) {
	suite.Run(t, new(GlobalLoggerSuite))
}
