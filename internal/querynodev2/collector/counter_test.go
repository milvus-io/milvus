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

package collector

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type CounterTestSuite struct {
	suite.Suite
	label   string
	counter *counter
}

func (suite *CounterTestSuite) SetupSuite() {
	suite.counter = newCounter()
	suite.label = "test_label"
}

func (suite *CounterTestSuite) TestBasic() {
	//get default value(zero)
	value := suite.counter.Get(suite.label)
	suite.Equal(int64(0), value)

	//get after inc
	suite.counter.Inc(suite.label, 3)
	value = suite.counter.Get(suite.label)
	suite.Equal(int64(3), value)

	//remove
	suite.counter.Remove(suite.label)
	value = suite.counter.Get(suite.label)
	suite.Equal(int64(0), value)

	//get after dec
	suite.counter.Dec(suite.label, 3)
	value = suite.counter.Get(suite.label)
	suite.Equal(int64(-3), value)

	//remove
	suite.counter.Remove(suite.label)
	value = suite.counter.Get(suite.label)
	suite.Equal(int64(0), value)
}

func TestCounter(t *testing.T) {
	suite.Run(t, new(CounterTestSuite))
}
