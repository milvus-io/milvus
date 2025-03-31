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

package metautil

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ChannelSuite struct {
	suite.Suite
}

func (s *ChannelSuite) TestParseChannel() {
	type testCase struct {
		tag             string
		virtualName     string
		expectError     bool
		expPhysical     string
		expCollectionID int64
		expShardIdx     int64
	}

	cases := []testCase{
		{
			tag:             "valid_virtual1",
			virtualName:     "by-dev-rootcoord-dml_0_449413615133917325v0",
			expectError:     false,
			expPhysical:     "by-dev-rootcoord-dml_0",
			expCollectionID: 449413615133917325,
			expShardIdx:     0,
		},
		{
			tag:             "valid_virtual2",
			virtualName:     "by-dev-rootcoord-dml_1_449413615133917325v1",
			expectError:     false,
			expPhysical:     "by-dev-rootcoord-dml_1",
			expCollectionID: 449413615133917325,
			expShardIdx:     1,
		},
		{
			tag:         "bad_format",
			virtualName: "by-dev-rootcoord-dml_2",
			expectError: true,
		},
		{
			tag:         "non_int_collection_id",
			virtualName: "by-dev-rootcoord-dml_0_collectionnamev0",
			expectError: true,
		},
		{
			tag:         "non_int_shard_idx",
			virtualName: "by-dev-rootcoord-dml_1_449413615133917325vunknown",
			expectError: true,
		},
	}

	mapper := NewDynChannelMapper()

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			channel, err := ParseChannel(tc.virtualName, mapper)
			if tc.expectError {
				s.Error(err)
				return
			}

			s.Equal(tc.expPhysical, channel.PhysicalName())
			s.Equal(tc.expCollectionID, channel.collectionID)
			s.Equal(tc.expShardIdx, channel.shardIdx)
			s.Equal(tc.virtualName, channel.VirtualName())
		})
	}
}

func (s *ChannelSuite) TestCompare() {
	virtualName1 := "by-dev-rootcoord-dml_0_449413615133917325v0"
	virtualName2 := "by-dev-rootcoord-dml_1_449413615133917325v1"

	mapper := NewDynChannelMapper()
	channel1, err := ParseChannel(virtualName1, mapper)
	s.Require().NoError(err)
	channel2, err := ParseChannel(virtualName2, mapper)
	s.Require().NoError(err)
	channel3, err := ParseChannel(virtualName1, mapper)
	s.Require().NoError(err)

	s.False(channel1.Equal(channel2))
	s.False(channel2.Equal(channel1))
	s.True(channel1.Equal(channel3))

	s.True(channel1.EqualString(virtualName1))
	s.False(channel1.EqualString(virtualName2))
	s.False(channel1.EqualString("abc"))
}

func TestChannel(t *testing.T) {
	suite.Run(t, new(ChannelSuite))
}
