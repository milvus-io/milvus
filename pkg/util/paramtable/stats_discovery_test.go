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

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatsDiscoveryConfig(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	var cfg dataCoordConfig
	cfg.initStatsDiscovery(base)
	require.Equal(t, "poll", cfg.StatsDiscoveryMode.GetValue())
	require.Equal(t, 10*time.Minute, cfg.StatsDiscoveryReconcileInterval.GetAsDuration(time.Second))
	for _, mode := range []string{"poll", "shadow", "event"} {
		require.NoError(t, base.Save(cfg.StatsDiscoveryMode.Key, mode))
		require.Equal(t, mode, cfg.StatsDiscoveryMode.GetValue())
	}
	for _, mode := range []string{"", "EVENT", "invalid"} {
		require.NoError(t, base.Save(cfg.StatsDiscoveryMode.Key, mode))
		require.Panics(t, func() { cfg.StatsDiscoveryMode.GetValue() })
	}
	for _, value := range []string{"0", "-1", "0.5", "invalid", "86401"} {
		require.NoError(t, base.Save(cfg.StatsDiscoveryReconcileInterval.Key, value))
		require.Panics(t, func() { cfg.StatsDiscoveryReconcileInterval.GetAsInt() })
	}
	for _, name := range []string{"StatsDiscoveryMode", "StatsDiscoveryReconcileInterval"} {
		field, ok := reflect.TypeOf(&cfg).Elem().FieldByName(name)
		require.True(t, ok)
		require.Equal(t, "false", field.Tag.Get("refreshable"))
	}
}
