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

package datacoord

import (
	"encoding/json"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const compactionTargetPropertySegmentIDs = "segment_ids"

type rewriteRule struct {
	segmentIDs []int64
	expectedTS uint64
}

func newRewriteRule(target *datapb.CompactionTarget) (rewriteRule, error) {
	segmentIDs, err := parseCompactionTargetSegmentIDs(target)
	if err != nil {
		return rewriteRule{}, err
	}
	return rewriteRule{
		segmentIDs: segmentIDs,
		expectedTS: target.GetExpectedTS(),
	}, nil
}

func (rule rewriteRule) ScopeIn(segment *SegmentInfo) bool {
	if segment == nil {
		return false
	}
	if len(rule.segmentIDs) == 0 {
		return true
	}
	for _, segmentID := range rule.segmentIDs {
		if segment.GetID() == segmentID {
			return true
		}
	}
	return false
}

func (rule rewriteRule) Match(segment *SegmentInfo) bool {
	return segment != nil && segment.GetCreateTs() < rule.expectedTS
}

func sortedCompactionTargetSegmentIDs(segmentIDs []int64) []int64 {
	sorted := append([]int64(nil), segmentIDs...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sorted
}

func parseCompactionTargetSegmentIDs(target *datapb.CompactionTarget) ([]int64, error) {
	if target.GetIntent() != datapb.TargetIntent_INTENT_REWRITE {
		return nil, nil
	}
	raw := target.GetProperties()[compactionTargetPropertySegmentIDs]
	if raw == "" {
		return nil, nil
	}
	var segmentIDs []int64
	if err := json.Unmarshal([]byte(raw), &segmentIDs); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid compaction target segment_ids property: %v", err)
	}
	return sortedCompactionTargetSegmentIDs(segmentIDs), nil
}

func compactionTargetSegmentIDs(target *datapb.CompactionTarget) ([]int64, bool) {
	segmentIDs, err := parseCompactionTargetSegmentIDs(target)
	if err != nil {
		return nil, false
	}
	return segmentIDs, true
}

func compactionTargetSegmentIDProperties(segmentIDs []int64) map[string]string {
	if len(segmentIDs) == 0 {
		return nil
	}
	value, err := json.Marshal(sortedCompactionTargetSegmentIDs(segmentIDs))
	if err != nil {
		return nil
	}
	return map[string]string{compactionTargetPropertySegmentIDs: string(value)}
}
