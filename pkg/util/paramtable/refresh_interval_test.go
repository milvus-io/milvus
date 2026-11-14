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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewBaseTable_TestBuildDoesNotRefreshByDefault(t *testing.T) {
	mainTable := NewBaseTable(SkipRemote(true))
	t.Cleanup(mainTable.mgr.Close)
	require.Zero(t, mainTable.config.refreshInterval,
		"test builds must not refresh configuration in the background by default")

	hook := NewBaseTableFromYamlOnly(hookYamlFile)
	t.Cleanup(hook.mgr.Close)
	require.Zero(t, hook.config.refreshInterval,
		"the hook table must follow the same default")

	refreshing := NewBaseTable(SkipRemote(true), Interval(time.Hour))
	t.Cleanup(refreshing.mgr.Close)
	require.Equal(t, time.Hour, refreshing.config.refreshInterval,
		"an explicit Interval must still enable refreshing")
}
