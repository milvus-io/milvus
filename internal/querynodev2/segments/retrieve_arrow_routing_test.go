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

package segments

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestShouldUseArrowTransport pins every exclusion in the routing predicate.
//
// These matter more than their size suggests. The aggregation exclusion is what
// keeps a duplicate-field-id result off the Arrow path -- MaterializeArrowSelection
// reassembles columns BY FIELD ID, and an aggregation result can repeat an id,
// so routing one here would misattribute data rather than fail. The ORDER BY
// exclusion keeps results off a path whose reduce reports a selection instead of
// materializing values the sort needs. Both were previously asserted only in
// comments.
//
// Constructing the plan with SetIgnoreNonPk touches no C memory (ignoreNonPk is
// a plain Go field read by IsIgnoreNonPk), so this needs no loaded segment.
func TestShouldUseArrowTransport(t *testing.T) {
	paramtable.Init()

	aggregate := []*planpb.Aggregate{{Op: planpb.AggregateOp_count}}

	for _, tc := range []struct {
		name        string
		zeroCopy    bool
		groupBy     []int64
		aggregates  []*planpb.Aggregate
		orderBy     []*planpb.OrderByField
		ignoreNonPk bool
		want        bool
		why         string
	}{
		{
			name: "plain requery routes to arrow", zeroCopy: true, want: true,
			why: "the shape the transport exists for",
		},
		{
			name: "flag off", zeroCopy: false, want: false,
			why: "common.interface.zeroCopy is the rollback mechanism and must gate everything",
		},
		{
			name: "group by", zeroCopy: true, groupBy: []int64{101}, want: false,
			why: "an aggregation result can repeat a field id, which the id-keyed reassembly cannot represent",
		},
		{
			name: "aggregates", zeroCopy: true, aggregates: aggregate, want: false,
			why: "count(*) and friends arrive as aggregates; this is the check that excludes them",
		},
		{
			name: "order by", zeroCopy: true,
			orderBy: []*planpb.OrderByField{{FieldId: 104, Ascending: true}}, want: false,
			why: "OrderByLimitOperator sorts by field VALUES, which a lazy selection has not materialized",
		},
		{
			name: "ignore non pk", zeroCopy: true, ignoreNonPk: true, want: false,
			why: "every user column is withheld, so the Arrow batch would carry nothing",
		},
		{
			name: "flag off wins over an otherwise eligible request", zeroCopy: false,
			want: false,
			why:  "no combination may route to Arrow with the flag down",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paramtable.Get().Save(
				paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled.Key,
				boolStr(tc.zeroCopy))
			defer paramtable.Get().Reset(
				paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled.Key)

			req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{
				GroupByFieldIds: tc.groupBy,
				Aggregates:      tc.aggregates,
				OrderByFields:   tc.orderBy,
			}}
			plan := &segcore.RetrievePlan{}
			plan.SetIgnoreNonPk(tc.ignoreNonPk)

			require.Equal(t, tc.want, shouldUseArrowTransport(req, plan), tc.why)
		})
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
