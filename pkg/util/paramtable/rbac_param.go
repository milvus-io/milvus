package paramtable

import (
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
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
		Key:          "common.security.rbac.overrideBuiltInPrivilegeGroups.enabled",
		DefaultValue: "false",
		Version:      "2.4.16",
		Doc:          "Whether to override build-in privilege groups",
		Export:       true,
	}
	p.Enabled.Init(base.mgr)

	p.ClusterReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.readonly.privileges",
		DefaultValue: strings.Join(util.ClusterReadOnlyPrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level readonly privileges",
		Export:       true,
	}
	p.ClusterReadOnlyPrivileges.Init(base.mgr)

	p.ClusterReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.readwrite.privileges",
		DefaultValue: strings.Join(util.ClusterReadWritePrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level readwrite privileges",
		Export:       true,
	}
	p.ClusterReadWritePrivileges.Init(base.mgr)

	p.ClusterAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.admin.privileges",
		DefaultValue: strings.Join(util.ClusterAdminPrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level admin privileges",
		Export:       true,
	}
	p.ClusterAdminPrivileges.Init(base.mgr)

	p.DBReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.database.readonly.privileges",
		DefaultValue: strings.Join(util.DatabaseReadOnlyPrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Database level readonly privileges",
		Export:       true,
	}
	p.DBReadOnlyPrivileges.Init(base.mgr)

	p.DBReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.database.readwrite.privileges",
		DefaultValue: strings.Join(util.DatabaseReadWritePrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Database level readwrite privileges",
		Export:       true,
	}
	p.DBReadWritePrivileges.Init(base.mgr)

	p.DBAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.database.admin.privileges",
		DefaultValue: strings.Join(util.DatabaseAdminPrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Database level admin privileges",
		Export:       true,
	}
	p.DBAdminPrivileges.Init(base.mgr)

	p.CollectionReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.collection.readonly.privileges",
		DefaultValue: strings.Join(util.CollectionReadOnlyPrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Collection level readonly privileges",
		Export:       true,
	}
	p.CollectionReadOnlyPrivileges.Init(base.mgr)

	p.CollectionReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.collection.readwrite.privileges",
		DefaultValue: strings.Join(util.CollectionReadWritePrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Collection level readwrite privileges",
		Export:       true,
	}
	p.CollectionReadWritePrivileges.Init(base.mgr)

	p.CollectionAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.collection.admin.privileges",
		DefaultValue: strings.Join(util.CollectionAdminPrivileges, ","),
		Version:      "2.4.16",
		Doc:          "Collection level admin privileges",
		Export:       true,
	}
	p.CollectionAdminPrivileges.Init(base.mgr)
}

func (p *rbacConfig) GetDefaultPrivilegeGroups() []*milvuspb.PrivilegeGroupInfo {
	privilegeGroupConfigs := []struct {
		GroupName  string
		Privileges func() []string
	}{
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterReadOnly.String()), p.ClusterReadOnlyPrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterReadWrite.String()), p.ClusterReadWritePrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterAdmin.String()), p.ClusterAdminPrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseReadOnly.String()), p.DBReadOnlyPrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseReadWrite.String()), p.DBReadWritePrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseAdmin.String()), p.DBAdminPrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadOnly.String()), p.CollectionReadOnlyPrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadWrite.String()), p.CollectionReadWritePrivileges.GetAsStrings},
		{util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionAdmin.String()), p.CollectionAdminPrivileges.GetAsStrings},
	}

	builtinGroups := make([]*milvuspb.PrivilegeGroupInfo, 0, len(privilegeGroupConfigs))
	for _, config := range privilegeGroupConfigs {
		privileges := lo.Map(config.Privileges(), func(name string, _ int) *milvuspb.PrivilegeEntity {
			return &milvuspb.PrivilegeEntity{Name: name}
		})
		builtinGroups = append(builtinGroups, &milvuspb.PrivilegeGroupInfo{
			GroupName:  config.GroupName,
			Privileges: privileges,
		})
	}
	return builtinGroups
}

func (p *rbacConfig) GetDefaultPrivilegeGroup(privName string) *milvuspb.PrivilegeGroupInfo {
	for _, group := range p.GetDefaultPrivilegeGroups() {
		if group.GetGroupName() == privName {
			return group
		}
	}
	return nil
}

func (p *rbacConfig) GetDefaultPrivilegeGroupPrivileges(groupName string) []string {
	group := p.GetDefaultPrivilegeGroup(groupName)
	if group == nil {
		return nil
	}
	return lo.Map(group.GetPrivileges(), func(priv *milvuspb.PrivilegeEntity, _ int) string {
		return priv.GetName()
	})
}

func (p *rbacConfig) GetDefaultPrivilegeGroupNames() []string {
	return lo.Map(p.GetDefaultPrivilegeGroups(), func(group *milvuspb.PrivilegeGroupInfo, _ int) string {
		return group.GroupName
	})
}

func (p *rbacConfig) IsCollectionPrivilegeGroup(privName string) bool {
	for _, groupName := range p.GetDefaultPrivilegeGroupNames() {
		if strings.Contains(groupName, milvuspb.PrivilegeLevel_Collection.String()) && groupName == privName {
			return true
		}
	}
	return false
}
