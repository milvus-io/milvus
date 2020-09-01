// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestComparison(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMinMaxSuite{})

type testMinMaxSuite struct{}

func (s *testMinMaxSuite) TestMinUint64(c *C) {
	c.Assert(MinUint64(1, 2), Equals, uint64(1))
	c.Assert(MinUint64(2, 1), Equals, uint64(1))
	c.Assert(MinUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMaxUint64(c *C) {
	c.Assert(MaxUint64(1, 2), Equals, uint64(2))
	c.Assert(MaxUint64(2, 1), Equals, uint64(2))
	c.Assert(MaxUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMinDuration(c *C) {
	c.Assert(MinDuration(time.Minute, time.Second), Equals, time.Second)
	c.Assert(MinDuration(time.Second, time.Minute), Equals, time.Second)
	c.Assert(MinDuration(time.Second, time.Second), Equals, time.Second)
}
