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

package taskcost

import "time"

func NowMs() int64 {
	return time.Now().UnixMilli()
}

func CalcCostTimeMs(startMs, endMs int64) int64 {
	if startMs <= 0 || endMs <= 0 || endMs < startMs {
		return 0
	}
	return endMs - startMs
}

func EstimateConcurrentWorkers(parallel, poolCap int64) int64 {
	if parallel <= 0 {
		parallel = 1
	}
	if poolCap <= 0 {
		return parallel
	}
	if parallel > poolCap {
		return poolCap
	}
	return parallel
}
