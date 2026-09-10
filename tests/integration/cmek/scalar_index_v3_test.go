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

package cmek

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestScalarIndexV3Campaign(t *testing.T) {
	suite.Run(t, new(ScalarIndexV3Suite))
}

func (s *ScalarIndexV3Suite) SetupSuite() {
	s.setup(packedV3Campaign)
}

func (s *ScalarIndexV3Suite) TearDownSuite() {
	s.tearDown()
}

func (s *ScalarIndexV3Suite) TestScalarSTLSortV3() {
	s.runCell(int64RangeCell("stl_sort", "STL_SORT"))
}

func (s *ScalarIndexV3Suite) TestScalarTrieV3() {
	s.runCell(trieCell())
}

func (s *ScalarIndexV3Suite) TestScalarBitmapV3() {
	s.runCell(bitmapCell())
}

func (s *ScalarIndexV3Suite) TestScalarHybridV3() {
	s.runCell(int64RangeCell("hybrid", "HYBRID"))
}

func (s *ScalarIndexV3Suite) TestScalarInvertedV3() {
	s.runCell(int64RangeCell("inverted", "INVERTED"))
}

func (s *ScalarIndexV3Suite) TestScalarNGRAMV3() {
	s.runCell(ngramCell())
}

func (s *ScalarIndexV3Suite) TestScalarRTREEV3() {
	s.runCell(geometryCell())
}

func (s *ScalarIndexV3Suite) TestScalarTextMatchV3() {
	s.runCell(textMatchCell())
}
