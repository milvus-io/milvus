package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/config"
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
