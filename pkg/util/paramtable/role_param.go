package paramtable

import (
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
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
