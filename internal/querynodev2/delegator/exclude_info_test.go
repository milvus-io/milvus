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

package delegator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ExcludedInfoSuite struct {
	suite.Suite

	excludedSegments ExcludedSegments
}

func (s *ExcludedInfoSuite) SetupSuite() {
	s.excludedSegments = *NewExcludedSegments(1 * time.Second)
}

func (s *ExcludedInfoSuite) TestBasic() {
	s.excludedSegments.Insert(map[int64]uint64{
		1: 3,
	})

	s.False(s.excludedSegments.Verify(1, 1))
	s.True(s.excludedSegments.Verify(1, 4))

	time.Sleep(1 * time.Second)

	s.True(s.excludedSegments.ShouldClean())
	s.excludedSegments.CleanInvalid(5)
	s.Len(s.excludedSegments.segments, 0)

	s.True(s.excludedSegments.Verify(1, 1))
	s.True(s.excludedSegments.Verify(1, 4))
}

func TestExcludedInfoSuite(t *testing.T) {
	suite.Run(t, new(ExcludedInfoSuite))
}
