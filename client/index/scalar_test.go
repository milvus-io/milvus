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

package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ScalarIndexSuite struct {
	suite.Suite
}

func (s *ScalarIndexSuite) TestConstructors() {
	type testCase struct {
		tag             string
		input           Index
		expectIndexType IndexType
	}

	testcases := []testCase{
		{tag: "Trie", input: NewTrieIndex(), expectIndexType: Trie},
		{tag: "Sorted", input: NewSortedIndex(), expectIndexType: Sorted},
		{tag: "Inverted", input: NewInvertedIndex(), expectIndexType: Inverted},
		{tag: "Bitmap", input: NewBitmapIndex(), expectIndexType: BITMAP},
	}

	for _, tc := range testcases {
		s.Run(fmt.Sprintf("%s_indextype", tc.tag), func() {
			s.Equal(tc.expectIndexType, tc.input.IndexType())
		})
	}

	for _, tc := range testcases {
		s.Run(fmt.Sprintf("%s_params", tc.tag), func() {
			params := tc.input.Params()
			itv, ok := params[IndexTypeKey]
			if s.True(ok) {
				s.EqualValues(tc.expectIndexType, itv)
			}
		})
	}
}

func TestScalarIndexes(t *testing.T) {
	suite.Run(t, new(ScalarIndexSuite))
}
