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

package dist

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadReportUpdatesWhenDistributionIsUnchanged(t *testing.T) {
	paramtable.Init()
	manager := session.NewNodeManager()
	node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
	manager.Add(node)
	node.RequireLoadReport()
	dh := &distHandler{nodeManager: manager, dist: meta.NewDistributionManager(manager), lastUpdateTs: 10}
	// Initial full report with an unchanged timestamp still establishes readiness
	// because the restored distribution is already at the matching timestamp.
	dh.handleDistResp(context.Background(), &querypb.GetDataDistributionResponse{NodeID: 1, LastModifyTs: 10, LoadResourceReportSupported: true,
		LoadResourceReport: &querypb.QueryNodeLoadResourceReport{LoadingSegments: []*querypb.LoadingSegmentInfo{{SegmentId: 7}}}})
	require.True(t, node.LoadAdmissionReady())
	require.Len(t, node.LoadResourceReport().GetLoadingSegments(), 1)
	dh.handleDistResp(context.Background(), &querypb.GetDataDistributionResponse{NodeID: 1, LastModifyTs: 10, LoadResourceReportSupported: true, LoadResourceReport: &querypb.QueryNodeLoadResourceReport{}})
	require.Empty(t, node.LoadResourceReport().GetLoadingSegments(), "resource-only changes must not be skipped with the distribution")
	dh.handleDistResp(context.Background(), &querypb.GetDataDistributionResponse{NodeID: 1, LastModifyTs: 10, LoadResourceReportSupported: true})
	require.False(t, node.LoadAdmissionReady(), "sampling error must close admission")
}
