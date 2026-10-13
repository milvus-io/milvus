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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The report belongs to this NodeInfo (one QN session), never to a reused address.
type loadReportState struct {
	required                 bool
	fullDistributionReceived bool
	resourceReportSupported  bool
	resourceReport           *querypb.QueryNodeLoadResourceReport
	reportedAt               time.Time
}

// BeginLoadReportSync closes admission until an initial full distribution has
// been received. After the bounded startup wait, only synchronized nodes admit
// new work; missing reports are never interpreted as empty nodes.
func (m *NodeManager) BeginLoadReportSync(timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reportSyncStarted = time.Now()
	m.reportSyncTimeout = timeout
	for _, node := range m.nodes {
		node.RequireLoadReport()
	}
}

func (m *NodeManager) LoadReportSyncReady() bool {
	return m.loadReportSyncReadyAt(time.Now())
}

func (m *NodeManager) loadReportSyncReadyAt(now time.Time) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.reportSyncStarted.IsZero() {
		return true
	}
	for _, node := range m.nodes {
		if !node.LoadAdmissionReady() {
			return now.Sub(m.reportSyncStarted) >= m.reportSyncTimeout
		}
	}
	return true
}

func (n *NodeInfo) RequireLoadReport() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.required = true
	n.fullDistributionReceived = false
}

// UpdateLoadReport is called after distribution reconciliation. A resource-only
// heartbeat updates loading state even if the segment distribution did not change.
func (n *NodeInfo) UpdateLoadReport(report *querypb.QueryNodeLoadResourceReport, supported, full bool, now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fullDistributionReceived = n.fullDistributionReceived || full
	n.resourceReportSupported = n.resourceReportSupported || supported
	if report != nil {
		n.resourceReportSupported = true
		n.resourceReport = proto.Clone(report).(*querypb.QueryNodeLoadResourceReport)
	} else {
		n.resourceReport = nil
	}
	n.reportedAt = now
}

func (n *NodeInfo) LoadResourceReport() *querypb.QueryNodeLoadResourceReport {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.resourceReport == nil {
		return nil
	}
	return proto.Clone(n.resourceReport).(*querypb.QueryNodeLoadResourceReport)
}

func (n *NodeInfo) LoadAdmissionReady() bool {
	return n.loadAdmissionReadyAt(time.Now())
}

func (n *NodeInfo) loadAdmissionReadyAt(now time.Time) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.required {
		return true
	}
	if !n.fullDistributionReceived || n.resourceReportSupported && n.resourceReport == nil {
		return false
	}
	interval := paramtable.Get().QueryCoordCfg.HeartbeatAvailableInterval.GetAsDuration(time.Millisecond)
	return interval > 0 && now.Sub(n.reportedAt) <= interval
}
