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

package extension

// QueryHook is the search-parameter tuning hook the QueryNode consults before
// every search: the same method set a queryNode.soPath plug-in exports as its
// QueryNodePlugin symbol, spelled here so that a distribution can compile one
// into the binary instead. Init receives autoIndex.params.search,
// InitTuningConfig and DeleteTuningConfig follow autoIndex.params.tuning, Run
// tunes one search and CalculateEffectiveSegmentNum sizes it.
type QueryHook interface {
	Run(map[string]any) error
	Init(string) error
	InitTuningConfig(map[string]string) error
	DeleteTuningConfig(string) error
	CalculateEffectiveSegmentNum(rowCounts []int64, topk int64) int
}

// SetQueryHook installs a compiled-in query hook. The QueryNode prefers it
// over queryNode.soPath and refuses a deployment that configures both. Call it
// before milvus starts; a nil hook leaves the stock behavior in place. It is
// independent of SetHook and of FormInstalled: it marks nothing but itself.
func SetQueryHook(h QueryHook) {
	installedQueryHook.Store(&queryHookBox{hook: h})
}

// InstalledQueryHook returns the installed query hook, or nil.
func InstalledQueryHook() QueryHook {
	if b := installedQueryHook.Load(); b != nil {
		return b.hook
	}
	return nil
}
