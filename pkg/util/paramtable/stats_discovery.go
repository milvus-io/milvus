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

package paramtable

import "strconv"

func (p *dataCoordConfig) initStatsDiscovery(base *BaseTable) {
	p.StatsDiscoveryMode = ParamItem{
		Key: "dataCoord.statsInspector.discoveryMode", Version: "3.0.0",
		DefaultValue: "poll", Export: true,
		Doc: "Stats task discovery: poll (legacy), shadow (read-only events), or event. Takes effect on restart.",
		Formatter: func(value string) string {
			switch value {
			case "poll", "shadow", "event":
				return value
			default:
				panic("dataCoord.statsInspector.discoveryMode must be poll, shadow, or event")
			}
		},
	}
	p.StatsDiscoveryMode.Init(base.mgr)
	p.StatsDiscoveryReconcileInterval = ParamItem{
		Key: "dataCoord.statsInspector.reconcileInterval", Version: "3.0.0",
		DefaultValue: "600", Export: true,
		Doc: "Seconds between stats reconciliation requests. Active scans are not restarted. Takes effect on restart.",
		Formatter: func(value string) string {
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 || n > 86400 {
				panic("dataCoord.statsInspector.reconcileInterval must be an integer between 1 and 86400")
			}
			return value
		},
	}
	p.StatsDiscoveryReconcileInterval.Init(base.mgr)
}
