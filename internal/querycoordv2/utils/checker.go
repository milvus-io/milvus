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

package utils

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	SegmentCheckerName = "segment_checker"
	ChannelCheckerName = "channel_checker"
	BalanceCheckerName = "balance_checker"
	IndexCheckerName   = "index_checker"
	LeaderCheckerName  = "leader_checker"
	ManualBalanceName  = "manual_balance"
	StatsCheckerName   = "stats_checker"
)

type CheckerType int32

const (
	ChannelChecker CheckerType = iota + 1
	SegmentChecker
	BalanceChecker
	IndexChecker
	LeaderChecker
	ManualBalance
	StatsChecker
)

var checkerNames = map[CheckerType]string{
	SegmentChecker: SegmentCheckerName,
	ChannelChecker: ChannelCheckerName,
	BalanceChecker: BalanceCheckerName,
	IndexChecker:   IndexCheckerName,
	LeaderChecker:  LeaderCheckerName,
	ManualBalance:  ManualBalanceName,
	StatsChecker:   StatsCheckerName,
}

func (s CheckerType) String() string {
	return checkerNames[s]
}

func FilterReleased[E interface{ GetCollectionID() int64 }](elems []E, collections []int64) []E {
	collectionSet := typeutil.NewUniqueSet(collections...)
	ret := make([]E, 0, len(elems))
	for i := range elems {
		collection := elems[i].GetCollectionID()
		if !collectionSet.Contain(collection) {
			ret = append(ret, elems[i])
		}
	}
	return ret
}

func FindMaxVersionSegments(segments []*meta.Segment) []*meta.Segment {
	versions := make(map[int64]int64)
	segMap := make(map[int64]*meta.Segment)
	for _, s := range segments {
		v, ok := versions[s.GetID()]
		if !ok || v < s.Version {
			versions[s.GetID()] = s.Version
			segMap[s.GetID()] = s
		}
	}
	ret := make([]*meta.Segment, 0)
	for _, s := range segMap {
		ret = append(ret, s)
	}
	return ret
}
