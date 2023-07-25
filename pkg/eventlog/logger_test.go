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

type LoggerSuite struct {
	suite.Suite
}

func (s *LoggerSuite) TearDownTest() {
	global.Store(nil)
}

func (s *LoggerSuite) TestRecord() {
	mock1 := NewMockLogger(s.T())
	mock2 := NewMockLogger(s.T())

	Register("mock1", mock1)
	Register("mock2", mock2)

	rawEvt := NewRawEvt(Level_Info, "test")

	mock1.EXPECT().Record(rawEvt)
	mock2.EXPECT().Record(rawEvt)

	Record(rawEvt)

	mock3 := NewMockLogger(s.T())

	Register("mock3", mock3) // register logger without expectations

	rawEvt = NewRawEvt(Level_Debug, "test")

	Record(rawEvt)
}

func (s *LoggerSuite) TestRecordFunc() {
	mock1 := NewMockLogger(s.T())
	mock2 := NewMockLogger(s.T())

	Register("mock1", mock1)
	Register("mock2", mock2)

	rawEvt := NewRawEvt(Level_Info, "test")

	mock1.EXPECT().RecordFunc(mock.Anything, mock.Anything)
	mock2.EXPECT().RecordFunc(mock.Anything, mock.Anything)

	RecordFunc(Level_Info, func() Evt { return rawEvt })

	mock3 := NewMockLogger(s.T())

	Register("mock3", mock3) // register logger without expectations

	rawEvt = NewRawEvt(Level_Debug, "test")

	RecordFunc(Level_Debug, func() Evt { return rawEvt })
}

func (s *LoggerSuite) TestFlush() {
	mock1 := NewMockLogger(s.T())
	mock2 := NewMockLogger(s.T())

	Register("mock1", mock1)
	Register("mock2", mock2)

	mock1.EXPECT().Flush().Return(nil)
	mock2.EXPECT().Flush().Return(nil)

	err := Flush()
	s.NoError(err)
}

func TestLogger(t *testing.T) {
	suite.Run(t, new(LoggerSuite))
}
