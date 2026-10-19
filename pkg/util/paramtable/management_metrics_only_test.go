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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestManagementMetricsOnlyConfiguration(t *testing.T) {
	base := NewBaseTable(SkipRemote(true))
	params := &ComponentParam{}
	params.Init(base)
	assert.False(t, params.CommonCfg.ManagementMetricsOnly.GetAsBool())
	assert.False(t, base.Manager().IsImmutable(params.CommonCfg.ManagementMetricsOnly.Key),
		"do not persist the initial false default as an immutable etcd override")

	t.Setenv("COMMON_SECURITY_MANAGEMENTMETRICSONLY", "true")
	params = &ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	assert.True(t, params.CommonCfg.ManagementMetricsOnly.GetAsBool())
	assert.False(t, params.CommonCfg.AuthorizationEnabled.GetAsBool(),
		"management exposure is independent of data-plane authentication")
}
