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

package tasks

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

type SearchTaskSuite struct {
	suite.Suite
}

func (s *SearchTaskSuite) composePlaceholderGroup(nq int, dim int) []byte {
	placeHolderGroup := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:  "$0",
				Type: commonpb.PlaceholderType_FloatVector,
				Values: lo.RepeatBy(nq, func(_ int) []byte {
					bs := make([]byte, 0, dim*4)
					for j := 0; j < dim; j++ {
						var buffer bytes.Buffer
						f := rand.Float32()
						err := binary.Write(&buffer, common.Endian, f)
						s.Require().NoError(err)
						bs = append(bs, buffer.Bytes()...)
					}
					return bs
				}),
			},
		},
	}

	bs, err := proto.Marshal(placeHolderGroup)
	s.Require().NoError(err)
	return bs
}

func (s *SearchTaskSuite) composeEmptyPlaceholderGroup() []byte {
	placeHolderGroup := &commonpb.PlaceholderGroup{}

	bs, err := proto.Marshal(placeHolderGroup)
	s.Require().NoError(err)
	return bs
}

func (s *SearchTaskSuite) TestCombinePlaceHolderGroups() {
	s.Run("normal", func() {
		task := &SearchTask{
			placeholderGroup: s.composePlaceholderGroup(1, 128),
			others: []*SearchTask{
				{
					placeholderGroup: s.composePlaceholderGroup(1, 128),
				},
			},
		}

		task.combinePlaceHolderGroups()
	})

	s.Run("tasked_not_merged", func() {
		task := &SearchTask{}

		err := task.combinePlaceHolderGroups()
		s.NoError(err)
	})

	s.Run("empty_placeholdergroup", func() {
		task := &SearchTask{
			placeholderGroup: s.composeEmptyPlaceholderGroup(),
			others: []*SearchTask{
				{
					placeholderGroup: s.composePlaceholderGroup(1, 128),
				},
			},
		}

		err := task.combinePlaceHolderGroups()
		s.Error(err)

		task = &SearchTask{
			placeholderGroup: s.composePlaceholderGroup(1, 128),
			others: []*SearchTask{
				{
					placeholderGroup: s.composeEmptyPlaceholderGroup(),
				},
			},
		}

		err = task.combinePlaceHolderGroups()
		s.Error(err)
	})

	s.Run("unmarshal_fail", func() {
		task := &SearchTask{
			placeholderGroup: []byte{0x12, 0x34},
			others: []*SearchTask{
				{
					placeholderGroup: s.composePlaceholderGroup(1, 128),
				},
			},
		}

		err := task.combinePlaceHolderGroups()
		s.Error(err)

		task = &SearchTask{
			placeholderGroup: s.composePlaceholderGroup(1, 128),
			others: []*SearchTask{
				{
					placeholderGroup: []byte{0x12, 0x34},
				},
			},
		}

		err = task.combinePlaceHolderGroups()
		s.Error(err)
	})
}

func TestSearchTask(t *testing.T) {
	suite.Run(t, new(SearchTaskSuite))
}
