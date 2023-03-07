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

package errutil

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
)

type ErrSuite struct {
	suite.Suite
}

func (s *ErrSuite) TestCombine() {
	var (
		errFirst  = errors.New("first")
		errSecond = errors.New("second")
		errThird  = errors.New("third")
	)

	err := Combine(errFirst, errSecond)
	s.True(errors.Is(err, errFirst))
	s.True(errors.Is(err, errSecond))
	s.False(errors.Is(err, errThird))

	s.Equal("first: second", err.Error())
}

func (s *ErrSuite) TestCombineWithNil() {
	err := errors.New("non-nil")

	err = Combine(nil, err)
	s.NotNil(err)
}

func (s *ErrSuite) TestCombineOnlyNil() {
	err := Combine(nil, nil)
	s.Nil(err)
}

func TestErrors(t *testing.T) {
	suite.Run(t, new(ErrSuite))
}
