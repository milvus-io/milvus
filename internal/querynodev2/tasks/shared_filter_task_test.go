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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func groupedRequest(extras ...*internalpb.SubSearchRequest) *querypb.SearchRequest {
	return &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:                    &commonpb.MsgBase{MsgID: 7},
			CollectionID:            42,
			OutputFieldsId:          []int64{100},
			MvccTimestamp:           11,
			GuaranteeTimestamp:      12,
			TimeoutTimestamp:        13,
			Username:                "u",
			GroupByFieldIds:         []int64{5},
			IsIterator:              true,
			CollectionTtlTimestamps: 14,
			EntityTtlPhysicalTime:   15,
			// branch 0's own payload
			SerializedExprPlan: []byte("plan-0"),
			PlaceholderGroup:   []byte("ph-0"),
			Nq:                 3,
			Topk:               10,
			FieldId:            1,
			MetricType:         "L2",
		},
		DmlChannels:            []string{"ch-0"},
		SegmentIDs:             []int64{1, 2},
		Scope:                  querypb.DataScope_Historical,
		TotalChannelNum:        1,
		ExtraFilterSharingReqs: extras,
	}
}

func extraBranch(fieldID int64, nq, topk int64) *internalpb.SubSearchRequest {
	return &internalpb.SubSearchRequest{
		SerializedExprPlan: []byte("plan-x"),
		PlaceholderGroup:   []byte("ph-x"),
		Nq:                 nq,
		Topk:               topk,
		FieldId:            fieldID,
		MetricType:         "BM25",
		AnalyzerName:       "standard",
	}
}

func TestBuildSharedFilterBranches(t *testing.T) {
	req := groupedRequest(extraBranch(2, 5, 20))
	branches := buildSharedFilterBranches(req)
	require.Len(t, branches, 2)

	t.Run("branch 0 is the request itself", func(t *testing.T) {
		assert.Same(t, req, branches[0])
	})

	t.Run("an extra branch carries its own payload", func(t *testing.T) {
		got := branches[1].GetReq()
		assert.Equal(t, []byte("plan-x"), got.GetSerializedExprPlan())
		assert.Equal(t, []byte("ph-x"), got.GetPlaceholderGroup())
		assert.EqualValues(t, 5, got.GetNq())
		assert.EqualValues(t, 20, got.GetTopk())
		assert.EqualValues(t, 2, got.GetFieldId())
		assert.Equal(t, "BM25", got.GetMetricType())
		assert.Equal(t, "standard", got.GetAnalyzerName())
		assert.False(t, got.GetIsAdvanced())
		assert.EqualValues(t, common.PkFilterNoPkFilter, got.GetPkFilter())
	})

	t.Run("envelope fields are copied from the request", func(t *testing.T) {
		got := branches[1].GetReq()
		base := req.GetReq()
		assert.Equal(t, base.GetBase(), got.GetBase())
		assert.Equal(t, base.GetCollectionID(), got.GetCollectionID())
		assert.Equal(t, base.GetOutputFieldsId(), got.GetOutputFieldsId())
		assert.Equal(t, base.GetMvccTimestamp(), got.GetMvccTimestamp())
		assert.Equal(t, base.GetGuaranteeTimestamp(), got.GetGuaranteeTimestamp())
		assert.Equal(t, base.GetTimeoutTimestamp(), got.GetTimeoutTimestamp())
		assert.Equal(t, base.GetUsername(), got.GetUsername())
		assert.Equal(t, base.GetGroupByFieldIds(), got.GetGroupByFieldIds())
		assert.Equal(t, base.GetIsIterator(), got.GetIsIterator())
		assert.Equal(t, base.GetCollectionTtlTimestamps(), got.GetCollectionTtlTimestamps())
		assert.Equal(t, base.GetEntityTtlPhysicalTime(), got.GetEntityTtlPhysicalTime())
	})

	// The delegator's flattening does not carry Offset, ConsistencyLevel or
	// IsRecallEvaluation into a hybrid sub-request. Branch 0 therefore never
	// has them, and an extra branch must not either -- otherwise two branches
	// of the same group would be built differently from the same source.
	t.Run("field set matches branch 0 exactly", func(t *testing.T) {
		got := branches[1].GetReq()
		assert.Zero(t, got.GetOffset())
		assert.Zero(t, got.GetConsistencyLevel())
		assert.False(t, got.GetIsRecallEvaluation())
	})

	t.Run("routing fields follow the group", func(t *testing.T) {
		assert.Equal(t, req.GetDmlChannels(), branches[1].GetDmlChannels())
		assert.Equal(t, req.GetSegmentIDs(), branches[1].GetSegmentIDs())
		assert.Equal(t, req.GetScope(), branches[1].GetScope())
	})

	t.Run("an ungrouped request yields exactly one branch", func(t *testing.T) {
		assert.Len(t, buildSharedFilterBranches(groupedRequest()), 1)
	})
}

func TestSharedFilterTaskAccounting(t *testing.T) {
	newTask := func(req *querypb.SearchRequest) *SearchTask {
		return &SearchTask{
			req:         req,
			nq:          req.GetReq().GetNq(),
			topk:        req.GetReq().GetTopk(),
			originNqs:   []int64{req.GetReq().GetNq()},
			originTopks: []int64{req.GetReq().GetTopk()},
		}
	}

	t.Run("NQ sums every branch", func(t *testing.T) {
		// The scheduler counter feeds the proxy's load estimate, so a group
		// must report what it actually processes, not just branch 0.
		task := newTask(groupedRequest(extraBranch(2, 5, 20), extraBranch(3, 4, 20)))
		assert.EqualValues(t, 3+5+4, task.NQ())
	})

	t.Run("MinNQ spans every branch", func(t *testing.T) {
		task := newTask(groupedRequest(extraBranch(2, 1, 20)))
		assert.EqualValues(t, 1, task.MinNQ())
	})

	t.Run("ungrouped accounting is unchanged", func(t *testing.T) {
		task := newTask(groupedRequest())
		assert.EqualValues(t, 3, task.NQ())
		assert.EqualValues(t, 3, task.MinNQ())
	})

	// The NQ-axis merge (same plan, concatenated placeholder groups) and a
	// shared-filter group (same rows, different vector fields) are different
	// axes and must not compose.
	t.Run("a grouped task never merges", func(t *testing.T) {
		grouped := newTask(groupedRequest(extraBranch(2, 5, 20)))
		plain := newTask(groupedRequest())
		assert.False(t, grouped.Merge(plain))
		assert.False(t, plain.Merge(grouped))
	})
}
