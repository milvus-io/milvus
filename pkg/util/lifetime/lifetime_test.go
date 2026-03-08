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

package lifetime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type LifetimeSuite struct {
	suite.Suite
}

func (s *LifetimeSuite) TestNormal() {
	l := NewLifetime[int32](0)
	checkHealth := func(state int32) error {
		if state == 0 {
			return nil
		}
		return merr.WrapErrServiceNotReady("test", 0, "0")
	}

	state := l.GetState()
	s.EqualValues(0, state)

	s.NoError(l.Add(checkHealth))

	l.SetState(1)
	s.Error(l.Add(checkHealth))

	signal := make(chan struct{})
	go func() {
		l.Wait()
		close(signal)
	}()

	select {
	case <-signal:
		s.FailNow("signal closed before all tasks done")
	default:
	}

	l.Done()
	select {
	case <-signal:
	case <-time.After(time.Second):
		s.FailNow("signal not closed after all tasks done")
	}
}

func TestLifetime(t *testing.T) {
	suite.Run(t, new(LifetimeSuite))
}
