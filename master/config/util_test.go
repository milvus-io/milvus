// Copyright 2018 TiKV Project Authors.
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

package config

import (
	"github.com/czs007/suvlim/pkg/metapb"
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestValidateLabels(c *C) {
	tests := []struct {
		label  string
		hasErr bool
	}{
		{"z1", false},
		{"z-1", false},
		{"h1;", true},
		{"z_1", false},
		{"z_1&", true},
		{"cn", false},
		{"Zo^ne", true},
		{"z_", true},
		{"hos&t-15", true},
		{"_test1", true},
		{"-test1", true},
		{"192.168.199.1", false},
		{"www.pingcap.com", false},
		{"h_127.0.0.1", false},
		{"a", false},
		{"a/b", false},
		{"ab/", true},
		{"/ab", true},
		{"$abc", false},
		{"$", true},
		{"a$b", true},
		{"$$", true},
	}
	for _, t := range tests {
		c.Assert(ValidateLabels([]*metapb.StoreLabel{{Key: t.label}}) != nil, Equals, t.hasErr)
	}
}

func (s *testUtilSuite) TestValidateURLWithScheme(c *C) {
	tests := []struct {
		addr   string
		hasErr bool
	}{
		{"", true},
		{"foo", true},
		{"/foo", true},
		{"http", true},
		{"http://", true},
		{"http://foo", false},
		{"https://foo", false},
		{"http://127.0.0.1", false},
		{"http://127.0.0.1/", false},
		{"https://foo.com/bar", false},
		{"https://foo.com/bar/", false},
	}
	for _, t := range tests {
		c.Assert(ValidateURLWithScheme(t.addr) != nil, Equals, t.hasErr)
	}
}
