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

package compaction

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestCompactorCommonTaskSuite(t *testing.T) {
	suite.Run(t, new(CompactorCommonSuite))
}

type CompactorCommonSuite struct {
	suite.Suite
}

func (s *CompactorCommonSuite) TestEntityFilterByTTL() {
	milvusBirthday := getMilvusBirthday()

	tests := []struct {
		description string
		collTTL     int64
		nowTime     time.Time
		entityTime  time.Time

		expect bool
	}{
		// ttl == maxInt64, dur is 1hour, no entities should expire
		{"ttl=maxInt64, now<entity", math.MaxInt64, milvusBirthday, milvusBirthday.Add(time.Hour), false},
		{"ttl=maxInt64, now>entity", math.MaxInt64, milvusBirthday, milvusBirthday.Add(-time.Hour), false},
		{"ttl=maxInt64, now==entity", math.MaxInt64, milvusBirthday, milvusBirthday, false},
		// ttl == 0, no entities should expire
		{"ttl=0, now==entity", 0, milvusBirthday, milvusBirthday, false},
		{"ttl=0, now>entity", 0, milvusBirthday, milvusBirthday.Add(-time.Hour), false},
		{"ttl=0, now<entity", 0, milvusBirthday, milvusBirthday.Add(time.Hour), false},
		// ttl == 10days
		{"ttl=10days, nowTs-entityTs>10days", 864000000000000, milvusBirthday.AddDate(0, 0, 11), milvusBirthday, true},
		{"ttl=10days, nowTs-entityTs==10days", 864000000000000, milvusBirthday.AddDate(0, 0, 10), milvusBirthday, true},
		{"ttl=10days, nowTs-entityTs<10days", 864000000000000, milvusBirthday.AddDate(0, 0, 9), milvusBirthday, false},
		// ttl is maxInt64
		{"ttl=maxInt64, nowTs-entityTs>1000years", math.MaxInt64, milvusBirthday.AddDate(1000, 0, 11), milvusBirthday, true},
		{"ttl=maxInt64, nowTs-entityTs==1000years", math.MaxInt64, milvusBirthday.AddDate(1000, 0, 0), milvusBirthday, true},
		{"ttl=maxInt64, nowTs-entityTs==240year", math.MaxInt64, milvusBirthday.AddDate(240, 0, 0), milvusBirthday, false},
		{"ttl=maxInt64, nowTs-entityTs==maxDur", math.MaxInt64, milvusBirthday.Add(time.Duration(math.MaxInt64)), milvusBirthday, true},
		{"ttl<maxInt64, nowTs-entityTs==1000years", math.MaxInt64 - 1, milvusBirthday.AddDate(1000, 0, 0), milvusBirthday, true},
	}
	for _, test := range tests {
		s.Run(test.description, func() {
			filter := newEntityFilter(nil, test.collTTL, test.nowTime)

			entityTs := tsoutil.ComposeTSByTime(test.entityTime, 0)
			got := filter.Filtered("mockpk", entityTs)
			s.Equal(test.expect, got)

			if got {
				s.Equal(1, filter.GetExpiredCount())
				s.Equal(0, filter.GetDeletedCount())
			} else {
				s.Equal(0, filter.GetExpiredCount())
				s.Equal(0, filter.GetDeletedCount())
			}
		})
	}
}
