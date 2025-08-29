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
	"testing"

	"github.com/stretchr/testify/suite"
)

type RTreeIndexSuite struct {
	suite.Suite
}

func (s *RTreeIndexSuite) TestNewRTreeIndex() {
	idx := NewRTreeIndex()
	s.Equal(RTREE, idx.IndexType())

	params := idx.Params()
	s.Equal(string(RTREE), params[IndexTypeKey])
}

func (s *RTreeIndexSuite) TestNewRTreeIndexWithParams() {
	idx := NewRTreeIndexWithParams()
	s.Equal(RTREE, idx.IndexType())

	params := idx.Params()
	s.Equal(string(RTREE), params[IndexTypeKey])
}

func (s *RTreeIndexSuite) TestRTreeIndexBuilder() {
	idx := NewRTreeIndexBuilder().
		Build()

	s.Equal(RTREE, idx.IndexType())

	params := idx.Params()
	s.Equal(string(RTREE), params[IndexTypeKey])
}

func (s *RTreeIndexSuite) TestRTreeIndexBuilderDefaults() {
	idx := NewRTreeIndexBuilder().Build()
	s.Equal(RTREE, idx.IndexType())

	params := idx.Params()
	s.Equal(string(RTREE), params[IndexTypeKey])
}

func (s *RTreeIndexSuite) TestRTreeIndexBuilderChaining() {
	builder := NewRTreeIndexBuilder()

	// Test method chaining
	result := builder.Build()

	s.Equal(RTREE, result.IndexType())

	params := result.Params()
	s.Equal(string(RTREE), params[IndexTypeKey])
}

func TestRTreeIndex(t *testing.T) {
	suite.Run(t, new(RTreeIndexSuite))
}
