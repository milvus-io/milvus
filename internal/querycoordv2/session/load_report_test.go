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

package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadReportStartupWaitAndUnknownNodeIsolation(t *testing.T) {
	paramtable.Init()
	manager := NewNodeManager()
	a, b := NewNodeInfo(ImmutableNodeInfo{NodeID: 1}), NewNodeInfo(ImmutableNodeInfo{NodeID: 2})
	manager.Add(a)
	manager.Add(b)
	manager.BeginLoadReportSync(time.Second)
	now := manager.reportSyncStarted
	require.False(t, manager.loadReportSyncReadyAt(now))
	a.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, now)
	require.True(t, a.loadAdmissionReadyAt(now))
	require.False(t, manager.loadReportSyncReadyAt(now))
	require.True(t, manager.loadReportSyncReadyAt(now.Add(2*time.Second)))
	require.False(t, b.LoadAdmissionReady(), "startup timeout must not make an unreported node empty/ready")
	b.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, false, now)
	require.False(t, b.loadAdmissionReadyAt(now), "an incremental report cannot establish initial distribution")
	b.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, now)
	require.True(t, manager.loadReportSyncReadyAt(now))
	c := NewNodeInfo(ImmutableNodeInfo{NodeID: 3})
	manager.Add(c)
	require.False(t, c.LoadAdmissionReady(), "new sessions must synchronize independently")
}

func TestLoadReportCapabilitiesFailureFreshnessAndClone(t *testing.T) {
	paramtable.Init()
	node := NewNodeInfo(ImmutableNodeInfo{NodeID: 1})
	node.RequireLoadReport()
	now := time.Now()
	node.UpdateLoadReport(nil, true, true, now)
	require.False(t, node.loadAdmissionReadyAt(now), "first sampling error is unknown, not legacy zero usage")
	report := &querypb.QueryNodeLoadResourceReport{MemoryUsageBytes: 100, LoadingSegments: []*querypb.LoadingSegmentInfo{{SegmentId: 7}}}
	node.UpdateLoadReport(report, true, false, now)
	report.MemoryUsageBytes = 0
	require.EqualValues(t, 100, node.LoadResourceReport().GetMemoryUsageBytes())
	copy := node.LoadResourceReport()
	copy.LoadingSegments[0].SegmentId = 0
	require.EqualValues(t, 7, node.LoadResourceReport().GetLoadingSegments()[0].GetSegmentId())
	require.True(t, node.loadAdmissionReadyAt(now))
	require.False(t, node.loadAdmissionReadyAt(now.Add(time.Hour)))
	node.UpdateLoadReport(nil, true, false, now)
	require.False(t, node.loadAdmissionReadyAt(now))
	legacy := NewNodeInfo(ImmutableNodeInfo{NodeID: 2})
	legacy.RequireLoadReport()
	legacy.UpdateLoadReport(nil, false, true, now)
	require.True(t, legacy.loadAdmissionReadyAt(now), "old QNs retain distribution-only compatibility")
}
