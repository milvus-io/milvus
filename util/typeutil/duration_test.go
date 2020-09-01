// Copyright 2016 TiKV Project Authors.
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

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
)

var _ = Suite(&testDurationSuite{})

type testDurationSuite struct{}

type example struct {
	Interval Duration `json:"interval" toml:"interval"`
}

func (s *testDurationSuite) TestJSON(c *C) {
	example := &example{}

	text := []byte(`{"interval":"1h1m1s"}`)
	c.Assert(json.Unmarshal(text, example), IsNil)
	c.Assert(example.Interval.Seconds(), Equals, float64(60*60+60+1))

	b, err := json.Marshal(example)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, string(text))
}

func (s *testDurationSuite) TestTOML(c *C) {
	example := &example{}

	text := []byte(`interval = "1h1m1s"`)
	c.Assert(toml.Unmarshal(text, example), IsNil)
	c.Assert(example.Interval.Seconds(), Equals, float64(60*60+60+1))
}
