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
	"encoding/json"

	. "github.com/pingcap/check"
)

var _ = Suite(&testStringSliceSuite{})

type testStringSliceSuite struct {
}

func (s *testStringSliceSuite) TestJSON(c *C) {
	b := StringSlice([]string{"zone", "rack"})
	o, err := json.Marshal(b)
	c.Assert(err, IsNil)
	c.Assert(string(o), Equals, "\"zone,rack\"")

	var nb StringSlice
	err = json.Unmarshal(o, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, DeepEquals, b)
}

func (s *testStringSliceSuite) TestEmpty(c *C) {
	ss := StringSlice([]string{})
	b, err := json.Marshal(ss)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, "\"\"")

	var ss2 StringSlice
	c.Assert(ss2.UnmarshalJSON(b), IsNil)
	c.Assert(ss2, DeepEquals, ss)
}
