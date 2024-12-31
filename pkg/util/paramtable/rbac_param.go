package paramtable

import (
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util"
)

var (
	builtinPrivilegeGroups = map[string][]string{
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadOnly.String()):  collectionReadOnlyPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadWrite.String()): collectionReadWritePrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionAdmin.String()):     collectionAdminPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseReadOnly.String()):    databaseReadOnlyPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseReadWrite.String()):   databaseReadWritePrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseAdmin.String()):       databaseAdminPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterReadOnly.String()):     clusterReadOnlyPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterReadWrite.String()):    clusterReadWritePrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterAdmin.String()):        clusterAdminPrivilegeGroup,
	}

	collectionReadOnlyPrivilegeGroup = []string{
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeQuery.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSearch.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeIndexDetail.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetFlushState.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetLoadState.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeHasPartition.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeShowPartitions.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeAlias.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetStatistics.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListAliases.String()),
	}

	collectionReadWritePrivilegeGroup = append(collectionReadOnlyPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeLoad.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRelease.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeInsert.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDelete.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeUpsert.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeImport.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeFlush.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCompaction.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeLoadBalance.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateIndex.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropIndex.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreatePartition.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropPartition.String()),
	)

	collectionAdminPrivilegeGroup = append(collectionReadWritePrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateAlias.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropAlias.String()),
	)

	databaseReadOnlyPrivilegeGroup = []string{
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeShowCollections.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String()),
	}

	databaseReadWritePrivilegeGroup = append(databaseReadOnlyPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeAlterDatabase.String()),
	)

	databaseAdminPrivilegeGroup = append(databaseReadWritePrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateCollection.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropCollection.String()),
	)

	clusterReadOnlyPrivilegeGroup = []string{
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListDatabases.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSelectOwnership.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSelectUser.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeResourceGroup.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListResourceGroups.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListPrivilegeGroups.String()),
	}

	clusterReadWritePrivilegeGroup = append(clusterReadOnlyPrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeFlushAll.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeTransferNode.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeTransferReplica.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeUpdateResourceGroups.String()),
	)

	clusterAdminPrivilegeGroup = append(clusterReadWritePrivilegeGroup,
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeBackupRBAC.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRestoreRBAC.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateDatabase.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropDatabase.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateOwnership.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropOwnership.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeManageOwnership.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateResourceGroup.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropResourceGroup.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeUpdateUser.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRenameCollection.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreatePrivilegeGroup.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropPrivilegeGroup.String()),
		util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeOperatePrivilegeGroup.String()),
	)
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
		DefaultValue: strings.Join(clusterReadOnlyPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level readonly privileges",
		Export:       true,
	}
	p.ClusterReadOnlyPrivileges.Init(base.mgr)

	p.ClusterReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.readwrite.privileges",
		DefaultValue: strings.Join(clusterReadWritePrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level readwrite privileges",
		Export:       true,
	}
	p.ClusterReadWritePrivileges.Init(base.mgr)

	p.ClusterAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.cluster.admin.privileges",
		DefaultValue: strings.Join(clusterAdminPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Cluster level admin privileges",
		Export:       true,
	}
	p.ClusterAdminPrivileges.Init(base.mgr)

	p.DBReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.database.readonly.privileges",
		DefaultValue: strings.Join(databaseReadOnlyPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Database level readonly privileges",
		Export:       true,
	}
	p.DBReadOnlyPrivileges.Init(base.mgr)

	p.DBReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.database.readwrite.privileges",
		DefaultValue: strings.Join(databaseReadWritePrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Database level readwrite privileges",
		Export:       true,
	}
	p.DBReadWritePrivileges.Init(base.mgr)

	p.DBAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.database.admin.privileges",
		DefaultValue: strings.Join(databaseAdminPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Database level admin privileges",
		Export:       true,
	}
	p.DBAdminPrivileges.Init(base.mgr)

	p.CollectionReadOnlyPrivileges = ParamItem{
		Key:          "common.security.rbac.collection.readonly.privileges",
		DefaultValue: strings.Join(collectionReadOnlyPrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Collection level readonly privileges",
		Export:       true,
	}
	p.CollectionReadOnlyPrivileges.Init(base.mgr)

	p.CollectionReadWritePrivileges = ParamItem{
		Key:          "common.security.rbac.collection.readwrite.privileges",
		DefaultValue: strings.Join(collectionReadWritePrivilegeGroup, ","),
		Version:      "2.4.16",
		Doc:          "Collection level readwrite privileges",
		Export:       true,
	}
	p.CollectionReadWritePrivileges.Init(base.mgr)

	p.CollectionAdminPrivileges = ParamItem{
		Key:          "common.security.rbac.collection.admin.privileges",
		DefaultValue: strings.Join(collectionAdminPrivilegeGroup, ","),
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
		{"ClusterReadOnly", p.ClusterReadOnlyPrivileges.GetAsStrings},
		{"ClusterReadWrite", p.ClusterReadWritePrivileges.GetAsStrings},
		{"ClusterAdmin", p.ClusterAdminPrivileges.GetAsStrings},
		{"DatabaseReadOnly", p.DBReadOnlyPrivileges.GetAsStrings},
		{"DatabaseReadWrite", p.DBReadWritePrivileges.GetAsStrings},
		{"DatabaseAdmin", p.DBAdminPrivileges.GetAsStrings},
		{"CollectionReadOnly", p.CollectionReadOnlyPrivileges.GetAsStrings},
		{"CollectionReadWrite", p.CollectionReadWritePrivileges.GetAsStrings},
		{"CollectionAdmin", p.CollectionAdminPrivileges.GetAsStrings},
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
		if group.GroupName == privName {
			return group
		}
	}
	return nil
}

func (p *rbacConfig) GetDefaultPrivilegeGroupNames() []string {
	return lo.Keys(builtinPrivilegeGroups)
}

func (p *rbacConfig) IsCollectionPrivilegeGroup(privName string) bool {
	collectionPrivilegeGroups := lo.PickBy(builtinPrivilegeGroups, func(groupName string, _ []string) bool {
		return strings.Contains(groupName, "Collection")
	})
	_, exists := collectionPrivilegeGroups[privName]
	return exists
}
