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

//go:build !test

package paramtable

import "time"

// defaultRefreshInterval is how often a BaseTable re-reads its file and etcd
// sources when the Interval option does not set one. paramtable.Init passes
// MILVUS_CONFIG_REFRESH_INTERVAL as that option for the table it builds.
// Test builds use refresh_interval_testonly.go instead.
const defaultRefreshInterval = 5 * time.Second
