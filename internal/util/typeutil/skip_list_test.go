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

package typeutil

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"
)

type SkipListSuite struct {
	suite.Suite
}

func (s *SkipListSuite) TestNewSkipList() {
	s.Run("default", func() {
		sl, err := NewSkipList[uint64, string]()
		s.Require().NoError(err)
		s.Equal(defaultSkipListMaxLevel, sl.maxLevel)
		s.Equal(defaultSkipListSkip, sl.skip)
		s.Equal(0, sl.level)
		s.Equal(0, sl.length)
	})

	s.Run("with max level", func() {
		sl, err := NewSkipList[uint64, string](WithMaxLevel(4))
		s.Require().NoError(err)
		s.Equal(4, sl.maxLevel)
		s.Equal(defaultSkipListSkip, sl.skip)
		s.Equal(0, sl.level)
		s.Equal(0, sl.length)

		_, err = NewSkipList[uint64, string](WithMaxLevel(0))
		s.Error(err)
	})

	s.Run("with skip", func() {
		sl, err := NewSkipList[uint64, string](WithMaxLevel(4))
		s.Require().NoError(err)
		s.Equal(4, sl.maxLevel)
		s.Equal(defaultSkipListSkip, sl.skip)
		s.Equal(0, sl.level)
		s.Equal(0, sl.length)

		_, err = NewSkipList[uint64, string](WithMaxLevel(0))
		s.Error(err)
	})
}

func (s *SkipListSuite) TestGetInsert() {
	size := 100
	us := NewUniqueSet()
	dataset := make([]int64, 0, size)
	for i := 0; i < size; i++ {
		entry := rand.Int63()
		for us.Contain(entry) {
			entry = rand.Int63()
		}
		us.Insert(entry)
		dataset = append(dataset, entry)
	}

	sl, err := NewSkipList[int64, int64]()
	s.Require().NoError(err)

	for _, entry := range dataset {
		sl.Upsert(entry, -entry)
	}

	s.True(sl.sanityCheck())

	for _, entry := range dataset {
		value, ok := sl.Get(entry)
		s.True(ok)
		s.Equal(entry, -value)
	}
}

func (s *SkipListSuite) TestDelete() {
	size := 100
	us := NewUniqueSet()
	dataset := make([]int64, 0, size)
	for i := 0; i < size; i++ {
		entry := rand.Int63()
		for us.Contain(entry) {
			entry = rand.Int63()
		}
		us.Insert(entry)
		dataset = append(dataset, entry)
	}

	sl, err := NewSkipList[int64, int64]()
	s.Require().NoError(err)

	for _, entry := range dataset {
		sl.Upsert(entry, -entry)
	}

	s.Require().True(sl.sanityCheck())

	for _, entry := range dataset {
		value, ok := sl.Delete(entry)
		s.True(ok)
		s.Equal(entry, -value)

		s.True(sl.sanityCheck())

		_, ok = sl.Get(entry)
		s.False(ok)
	}
}

func (s *SkipListSuite) TestTruncateBefore() {
	s.Run("empty skip list", func() {
		sl, err := NewSkipList[int64, int64]()
		s.Require().NoError(err)

		s.NotPanics(func() {
			sl.TruncateBefore(0)
			sl.TruncateBefore(0)
		})
	})

	s.Run("normal usage", func() {
		sl, err := NewSkipList[int64, int64]()
		s.Require().NoError(err)

		sl.Upsert(1, -1)
		sl.Upsert(2, -2)

		s.T().Log("1")
		sl.TruncateBefore(0)
		s.T().Log("2")

		sl.Upsert(3, -3)

		s.T().Log("3")
		val, _ := sl.Get(3)
		s.EqualValues(-3, val)
	})
}

func TestSkipListSuite(t *testing.T) {
	suite.Run(t, new(SkipListSuite))
}
