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

	"github.com/milvus-io/milvus/pkg/v2/config"
)

func TestRoleConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.RoleCfg
	assert.Equal(t, cfg.Enabled.GetAsBool(), false)
	assert.Equal(t, cfg.Roles.GetValue(), "{}")
	assert.Equal(t, len(cfg.Roles.GetAsJSONMap()), 0)
}

func TestRoleConfig_Invalid(t *testing.T) {
	t.Run("valid roles", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("builtinRoles.enable", "true")
		mgr.SetConfig("builtinRoles.roles", `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`)
		p := &roleConfig{
			Enabled: ParamItem{
				Key: "builtinRoles.enable",
			},
			Roles: ParamItem{
				Key: "builtinRoles.roles",
			},
		}
		p.Enabled.Init(mgr)
		p.Roles.Init(mgr)
		assert.NotPanics(t, func() {
			p.panicIfNotValid(mgr)
		})
	})
	t.Run("invalid roles", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("builtinRoles.enable", "true")
		mgr.SetConfig("builtinRoles.roles", `{"db_admin": {"privileges": {"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}}}`)
		p := &roleConfig{
			Enabled: ParamItem{
				Key: "builtinRoles.enable",
			},
			Roles: ParamItem{
				Key: "builtinRoles.roles",
			},
		}
		p.Enabled.Init(mgr)
		p.Roles.Init(mgr)
		assert.Panics(t, func() {
			p.panicIfNotValid(mgr)
		})
	})
}
