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

type AverageCollectorTestSuite struct {
	suite.Suite
	label   string
	average *averageCollector
}

func (suite *AverageCollectorTestSuite) SetupSuite() {
	suite.average = newAverageCollector()
	suite.label = "test_label"
}

func (suite *AverageCollectorTestSuite) TestBasic() {
	// Get average not register
	_, err := suite.average.Average(suite.label)
	suite.Error(err)

	// register and get
	suite.average.Register(suite.label)
	value, err := suite.average.Average(suite.label)
	suite.Equal(float64(0), value)
	suite.NoError(err)

	// add and get
	sum := 4
	for i := 0; i <= sum; i++ {
		suite.average.Add(suite.label, float64(i))
	}
	value, err = suite.average.Average(suite.label)
	suite.NoError(err)
	suite.Equal(float64(sum)/2, value)
}

func TestAverageCollector(t *testing.T) {
	suite.Run(t, new(AverageCollectorTestSuite))
}
