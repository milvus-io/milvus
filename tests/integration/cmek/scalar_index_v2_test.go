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

func TestScalarIndexV2Campaign(t *testing.T) {
	suite.Run(t, new(ScalarIndexV2Suite))
}

func (s *ScalarIndexV2Suite) SetupSuite() {
	s.setup(legacyV2Campaign)
}

func (s *ScalarIndexV2Suite) TearDownSuite() {
	s.tearDown()
}

func (s *ScalarIndexV2Suite) TestScalarSTLSortV2() {
	s.runCell(int64RangeCell("stl_sort", "STL_SORT"))
}

func (s *ScalarIndexV2Suite) TestScalarTrieV2() {
	s.runCell(trieCell())
}

func (s *ScalarIndexV2Suite) TestScalarBitmapV2() {
	s.runCell(bitmapCell())
}

func (s *ScalarIndexV2Suite) TestScalarHybridV2() {
	s.runCell(int64RangeCell("hybrid", "HYBRID"))
}

func (s *ScalarIndexV2Suite) TestScalarInvertedV2() {
	s.runCell(int64RangeCell("inverted", "INVERTED"))
}

func (s *ScalarIndexV2Suite) TestScalarNGRAMV2() {
	s.runCell(ngramCell())
}

func (s *ScalarIndexV2Suite) TestScalarRTREEV2() {
	s.runCell(geometryCell())
}

func (s *ScalarIndexV2Suite) TestScalarTextMatchV2() {
	s.runCell(textMatchCell())
}
