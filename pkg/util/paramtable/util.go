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

// This file defines hook functions that are set by the upper layer (root module)
// to avoid importing root `internal` packages or CGO into the `pkg` submodule.

var (
	UpdateIndexSliceSize                      = func(size int) {}
	UpdateHighPriorityThreadCoreCoefficient   = func(coefficient float64) {}
	UpdateMiddlePriorityThreadCoreCoefficient = func(coefficient float64) {}
	UpdateLowPriorityThreadCoreCoefficient    = func(coefficient float64) {}

	UpdateDefaultOptimizeExprEnable         = func(enable bool) {}
	UpdateDefaultJSONKeyStatsCommitInterval = func(interval int) {}
	UpdateDefaultGrowingJSONKeyStatsEnable  = func(enable bool) {}
	UpdateDefaultConfigParamTypeCheck       = func(enable bool) {}

	UpdateLogLevel                 = func(level string) error { return nil }
	UpdateDefaultExprEvalBatchSize = func(size int) {}
)
