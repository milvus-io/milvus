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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMarshalRLSPlanSizeLimit(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.ProxyCfg.MaxMembershipFilterPlanSize.Key
	defer params.Reset(key)
	plan := &planpb.PlanNode{
		Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		OutputFieldIds: []int64{100},
	}
	size := int64(proto.Size(plan))

	params.Save(key, "1")
	serialized, accumulated, err := marshalPlanWithFilterSizeLimit(plan, 0, false)
	require.NoError(t, err)
	require.NotEmpty(t, serialized)
	require.Zero(t, accumulated)

	params.Save(key, strconv.FormatInt(size, 10))
	serialized, accumulated, err = marshalPlanWithFilterSizeLimit(plan, 0, true)
	require.NoError(t, err)
	require.NotEmpty(t, serialized)
	require.Equal(t, size, accumulated)

	serialized, unchanged, err := marshalPlanWithFilterSizeLimit(plan, accumulated, true)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.Equal(t, int32(1102), merr.Code(err))
	require.Nil(t, serialized)
	require.Equal(t, accumulated, unchanged)

	for _, invalid := range []string{"0", "-1", "invalid"} {
		params.Save(key, invalid)
		require.EqualValues(t, paramtable.DefaultMaxMembershipFilterPlanSize, params.ProxyCfg.MaxMembershipFilterPlanSize.GetAsInt64())
	}
}
