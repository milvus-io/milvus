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

package conc

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
)

type FutureSuite struct {
	suite.Suite
}

func (s *FutureSuite) TestFuture() {
	const sleepDuration = 200 * time.Millisecond
	errFuture := Go(func() (any, error) {
		time.Sleep(sleepDuration)
		return nil, errors.New("errFuture")
	})

	resultFuture := Go(func() (int, error) {
		time.Sleep(sleepDuration)
		return 10, nil
	})

	s.False(errFuture.OK())
	s.True(resultFuture.OK())
	s.Error(errFuture.Err())
	s.Equal(10, resultFuture.Value())
}

func TestFuture(t *testing.T) {
	suite.Run(t, new(FutureSuite))
}
