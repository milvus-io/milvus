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

package querynodev2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The config group reports what the node runs with: a non-refreshable item
// keeps its start value until a restart, so a later change in the config
// source must not show, while a refreshable item's change must.
func TestQueryNodeConfigEntriesReportStartValues(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	nonRefreshable := &params.QueryNodeCfg.MmapVectorField // refreshable:"false"
	refreshable := &params.QueryNodeCfg.ExprResCacheEnabled

	params.Save(nonRefreshable.Key, "true")
	params.Save(refreshable.Key, "true")
	t.Cleanup(func() {
		params.Reset(nonRefreshable.Key)
		params.Reset(refreshable.Key)
		startConfigValues.Range(func(k, _ any) bool {
			startConfigValues.Delete(k)
			return true
		})
	})
	captureStartConfig()
	_, captured := startConfigValues.Load(nonRefreshable.Key)
	require.True(t, captured, "a non-refreshable item is captured at start")
	_, captured = startConfigValues.Load(refreshable.Key)
	require.False(t, captured, "a refreshable item is read live")

	params.Save(nonRefreshable.Key, "false")
	params.Save(refreshable.Key, "false")
	entries := map[string]bool{}
	for _, e := range queryNodeConfigEntries() {
		entries[e.GetName()] = true
	}
	assert.Contains(t, entries, nonRefreshable.Key+"=true", "the start value, not the changed one")
	assert.Contains(t, entries, refreshable.Key+"=false", "the live value")
}
