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

//go:build test

package paramtable

import "time"

// defaultRefreshInterval is zero in test builds, so a BaseTable does not
// refresh its sources in the background unless asked to.
//
// Unit tests should change configuration through Save, Reset or Remove,
// which take effect immediately and fire the same config events. A
// background refresher only adds goroutines that re-read milvus.yaml and
// hook.yaml and poll etcd every few seconds: their allocations land in
// process-wide measurements such as testing.AllocsPerRun and
// runtime.MemStats, and their calls race with mockey patches of functions
// they use, such as time.Now and proto.Unmarshal.
//
// Tests that exercise refreshing pass the Interval option.
// MILVUS_CONFIG_REFRESH_INTERVAL only applies to the table paramtable.Init
// builds; the integration cluster sets it for the Milvus processes it starts.
const defaultRefreshInterval time.Duration = 0
