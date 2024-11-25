package paramtable

import (
	"strings"

	"github.com/milvus-io/milvus/pkg/util"
)

type rbacConfig struct {
	Enabled                    ParamItem `refreshable:"false"`
	ClusterReadOnlyPrivileges  ParamItem `refreshable:"false"`
	ClusterReadWritePrivileges ParamItem `refreshable:"false"`
	ClusterAdminPrivileges     ParamItem `refreshable:"false"`

	DBReadOnlyPrivileges  ParamItem `refreshable:"false"`
	DBReadWritePrivileges ParamItem `refreshable:"false"`
	DBAdminPrivileges     ParamItem `refreshable:"false"`

	CollectionReadOnlyPrivileges  ParamItem `refreshable:"false"`
	CollectionReadWritePrivileges ParamItem `refreshable:"false"`
	CollectionAdminPrivileges     ParamItem `refreshable:"false"`
}

func (p *rbacConfig) init(base *BaseTable) {
	p.Enabled = ParamItem{
		Key:          "common.security.rbac.overrideBuiltInPrivilgeGroups.enabled",
		DefaultValue: "false",
		Version:      "2.4.16",
		Doc:          "Whether to override build-in privilege groups",
		Export:       true,
	}
	p.Enabled.Init(base.mgr)

	p.ClusterReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.readonly.privileges",
		DefaultValue: strings.Join(util.ClusterReadOnlyPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level readonly privileges",
		Export:       true,
	}
	p.ClusterReadOnlyPrivileges.Init(base.mgr)

	p.ClusterReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.readwrite.privileges",
		DefaultValue: strings.Join(util.ClusterReadWritePrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level readwrite privileges",
		Export:       true,
	}
	p.ClusterReadWritePrivileges.Init(base.mgr)

	p.ClusterAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.admin.privileges",
		DefaultValue: strings.Join(util.ClusterAdminPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level admin privileges",
		Export:       true,
	}
	p.ClusterAdminPrivileges.Init(base.mgr)

	p.DBReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.database.readonly.privileges",
		DefaultValue: strings.Join(util.DatabaseReadOnlyPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Database level readonly privileges",
		Export:       true,
	}
	p.DBReadOnlyPrivileges.Init(base.mgr)

	p.DBReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.database.readwrite.privileges",
		DefaultValue: strings.Join(util.DatabaseReadWritePrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Database level readwrite privileges",
		Export:       true,
	}
	p.DBReadWritePrivileges.Init(base.mgr)

	p.DBAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.database.admin.privileges",
		DefaultValue: strings.Join(util.DatabaseAdminPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Database level admin privileges",
		Export:       true,
	}
	p.DBAdminPrivileges.Init(base.mgr)

	p.CollectionReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.collection.readonly.privileges",
		DefaultValue: strings.Join(util.CollectionReadOnlyPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Collection level readonly privileges",
		Export:       true,
	}
	p.CollectionReadOnlyPrivileges.Init(base.mgr)

	p.CollectionReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.collection.readwrite.privileges",
		DefaultValue: strings.Join(util.CollectionReadWritePrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Collection level readwrite privileges",
		Export:       true,
	}
	p.CollectionReadWritePrivileges.Init(base.mgr)

	p.CollectionAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.collection.admin.privileges",
		DefaultValue: strings.Join(util.CollectionAdminPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Collection level admin privileges",
		Export:       true,
	}
	p.CollectionAdminPrivileges.Init(base.mgr)
}
