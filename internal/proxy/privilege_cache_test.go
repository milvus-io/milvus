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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type PrivilegeCacheSuite struct {
	suite.Suite
}

func (s *PrivilegeCacheSuite) TearDownTest() {
	CleanPrivilegeCache()
}

func (s *PrivilegeCacheSuite) TestGetPrivilege() {
	// get current version
	_, _, version := GetPrivilegeCache("", "", "")
	SetPrivilegeCache("test-role", "test-object", "read", true, version)
	SetPrivilegeCache("test-role", "test-object", "delete", false, version)

	type testCase struct {
		tag            string
		input          [3]string
		expectIsPermit bool
		expectExists   bool
	}

	testCases := []testCase{
		{tag: "exist_true", input: [3]string{"test-role", "test-object", "read"}, expectIsPermit: true, expectExists: true},
		{tag: "exist_false", input: [3]string{"test-role", "test-object", "delete"}, expectIsPermit: false, expectExists: true},
		{tag: "not_exist", input: [3]string{"guest", "test-object", "delete"}, expectIsPermit: false, expectExists: false},
	}

	for _, tc := range testCases {
		s.Run(tc.tag, func() {
			isPermit, exists, _ := GetPrivilegeCache(tc.input[0], tc.input[1], tc.input[2])
			s.Equal(tc.expectIsPermit, isPermit)
			s.Equal(tc.expectExists, exists)
		})
	}
}

func (s *PrivilegeCacheSuite) TestSetPrivilegeVersion() {
	// get current version
	_, _, version := GetPrivilegeCache("", "", "")
	CleanPrivilegeCache()

	SetPrivilegeCache("test-role", "test-object", "read", true, version)

	isPermit, exists, nextVersion := GetPrivilegeCache("test-role", "test-object", "read")
	s.False(isPermit)
	s.False(exists)
	s.NotEqual(version, nextVersion)
}

func TestPrivilegeSuite(t *testing.T) {
	suite.Run(t, new(PrivilegeCacheSuite))
}
