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
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

type roleConfig struct {
	Enabled ParamItem `refreshable:"false"`
	Roles   ParamItem `refreshable:"false"`
}

func (p *roleConfig) init(base *BaseTable) {
	p.Enabled = ParamItem{
		Key:          "builtinRoles.enable",
		DefaultValue: "false",
		Version:      "2.3.4",
		Doc:          "Whether to init builtin roles",
		Export:       true,
	}
	p.Enabled.Init(base.mgr)

	p.Roles = ParamItem{
		Key:          "builtinRoles.roles",
		DefaultValue: `{}`,
		Version:      "2.3.4",
		Doc:          "what builtin roles should be init",
		Export:       true,
	}
	p.Roles.Init(base.mgr)

	p.panicIfNotValid(base.mgr)
}

func (p *roleConfig) panicIfNotValid(mgr *config.Manager) {
	if p.Enabled.GetAsBool() {
		m := p.Roles.GetAsRoleDetails()
		if m == nil {
			panic("builtinRoles.roles not invalid, should be json format")
		}

		j := funcutil.RoleDetailsToJSON(m)
		mgr.SetConfig("builtinRoles.roles", string(j))
	}
}
