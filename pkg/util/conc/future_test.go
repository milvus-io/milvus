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
	"go.uber.org/atomic"
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

func (s *FutureSuite) TestBlockOnAll() {
	cnt := atomic.NewInt32(0)
	futures := make([]*Future[struct{}], 10)
	for i := 0; i < 10; i++ {
		sleepTime := time.Duration(i) * 100 * time.Millisecond
		futures[i] = Go(func() (struct{}, error) {
			time.Sleep(sleepTime)
			cnt.Add(1)
			return struct{}{}, errors.New("errFuture")
		})
	}

	err := BlockOnAll(futures...)
	s.Error(err)
	s.Equal(int32(10), cnt.Load())

	cnt.Store(0)
	for i := 0; i < 10; i++ {
		sleepTime := time.Duration(i) * 100 * time.Millisecond
		futures[i] = Go(func() (struct{}, error) {
			time.Sleep(sleepTime)
			cnt.Add(1)
			return struct{}{}, nil
		})
	}

	err = BlockOnAll(futures...)
	s.NoError(err)
	s.Equal(int32(10), cnt.Load())
}

func (s *FutureSuite) TestAwaitAll() {
	cnt := atomic.NewInt32(0)
	futures := make([]*Future[struct{}], 10)
	for i := 0; i < 10; i++ {
		sleepTime := time.Duration(i) * 100 * time.Millisecond
		futures[i] = Go(func() (struct{}, error) {
			time.Sleep(sleepTime)
			cnt.Add(1)
			return struct{}{}, errors.New("errFuture")
		})
	}

	err := AwaitAll(futures...)
	s.Error(err)
	s.Equal(int32(1), cnt.Load())
}

func TestFuture(t *testing.T) {
	suite.Run(t, new(FutureSuite))
}
