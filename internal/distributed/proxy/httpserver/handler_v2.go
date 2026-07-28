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

package httpserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	validator "github.com/go-playground/validator/v10"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type HandlersV2 struct {
	proxy     types.ProxyComponent
	checkAuth bool
}

func NewHandlersV2(proxyClient types.ProxyComponent) *HandlersV2 {
	return &HandlersV2{
		proxy:     proxyClient,
		checkAuth: proxy.Params.CommonCfg.AuthorizationEnabled.GetAsBool(),
	}
}

var routeToMethod = map[string]string{ //nolint:gosec // not credentials, just a route-to-method mapping
	"/v2/vectordb/collections/list":                 "ShowCollections",
	"/v2/vectordb/collections/has":                  "HasCollection",
	"/v2/vectordb/collections/describe":             "DescribeCollection",
	"/v2/vectordb/collections/get_stats":            "GetCollectionStatistics",
	"/v2/vectordb/collections/get_load_state":       "GetLoadState",
	"/v2/vectordb/collections/create":               "CreateCollection",
	"/v2/vectordb/collections/drop":                 "DropCollection",
	"/v2/vectordb/collections/truncate":             "TruncateCollection",
	"/v2/vectordb/collections/rename":               "RenameCollection",
	"/v2/vectordb/collections/load":                 "LoadCollection",
	"/v2/vectordb/collections/refresh_load":         "LoadCollection",
	"/v2/vectordb/collections/release":              "ReleaseCollection",
	"/v2/vectordb/collections/alter_properties":     "AlterCollection",
	"/v2/vectordb/collections/add_function":         "AddCollectionFunction",
	"/v2/vectordb/collections/alter_function":       "AlterCollectionFunction",
	"/v2/vectordb/collections/drop_function":        "DropCollectionFunction",
	"/v2/vectordb/collections/add_function_field":   "AlterCollectionSchema",
	"/v2/vectordb/collections/drop_function_field":  "AlterCollectionSchema",
	"/v2/vectordb/collections/drop_properties":      "AlterCollection",
	"/v2/vectordb/collections/compact":              "ManualCompaction",
	"/v2/vectordb/collections/get_compaction_state": "GetCompactionState",
	"/v2/vectordb/collections/flush":                "Flush",

	"/v2/vectordb/collections/fields/alter_properties": "AlterCollectionField",
	"/v2/vectordb/collections/fields/add":              "AlterCollectionSchema",
	"/v2/vectordb/collections/fields/drop":             "AlterCollectionSchema",
	"/v2/vectordb/collections/struct_fields/add":       "AddCollectionStructField",

	"/v2/vectordb/databases/create":           "CreateDatabase",
	"/v2/vectordb/databases/drop":             "DropDatabase",
	"/v2/vectordb/databases/drop_properties":  "AlterDatabase",
	"/v2/vectordb/databases/list":             "ListDatabases",
	"/v2/vectordb/databases/describe":         "DescribeDatabase",
	"/v2/vectordb/databases/alter":            "AlterDatabase",
	"/v2/vectordb/databases/alter_properties": "AlterDatabase",

	"/v2/vectordb/entities/query":           "Query",
	"/v2/vectordb/entities/get":             "Query",
	"/v2/vectordb/entities/delete":          "Delete",
	"/v2/vectordb/entities/insert":          "Insert",
	"/v2/vectordb/entities/upsert":          "Upsert",
	"/v2/vectordb/entities/search":          "Search",
	"/v2/vectordb/entities/advanced_search": "HybridSearch",
	"/v2/vectordb/entities/hybrid_search":   "HybridSearch",

	"/v2/vectordb/partitions/list":      "ShowPartitions",
	"/v2/vectordb/partitions/has":       "HasPartition",
	"/v2/vectordb/partitions/get_stats": "GetPartitionStatistics",
	"/v2/vectordb/partitions/create":    "CreatePartition",
	"/v2/vectordb/partitions/drop":      "DropPartition",
	"/v2/vectordb/partitions/load":      "LoadPartitions",
	"/v2/vectordb/partitions/release":   "ReleasePartitions",

	"/v2/vectordb/users/list":            "ListCredUsers",
	"/v2/vectordb/users/describe":        "SelectUser",
	"/v2/vectordb/users/create":          "CreateCredential",
	"/v2/vectordb/users/update_password": "UpdateCredential",
	"/v2/vectordb/users/drop":            "DeleteCredential",
	"/v2/vectordb/users/grant_role":      "OperateUserRole",
	"/v2/vectordb/users/revoke_role":     "OperateUserRole",

	"/v2/vectordb/roles/list":                "SelectRole",
	"/v2/vectordb/roles/describe":            "SelectGrant",
	"/v2/vectordb/roles/create":              "CreateRole",
	"/v2/vectordb/roles/alter":               "AlterRole",
	"/v2/vectordb/roles/drop":                "DropRole",
	"/v2/vectordb/roles/grant_privilege":     "OperatePrivilege",
	"/v2/vectordb/roles/revoke_privilege":    "OperatePrivilege",
	"/v2/vectordb/roles/grant_privilege_v2":  "OperatePrivilegeV2",
	"/v2/vectordb/roles/revoke_privilege_v2": "OperatePrivilegeV2",

	"/v2/vectordb/privilege_groups/create":                       "CreatePrivilegeGroup",
	"/v2/vectordb/privilege_groups/drop":                         "DropPrivilegeGroup",
	"/v2/vectordb/privilege_groups/list":                         "ListPrivilegeGroups",
	"/v2/vectordb/privilege_groups/add_privileges_to_group":      "OperatePrivilegeGroup",
	"/v2/vectordb/privilege_groups/remove_privileges_from_group": "OperatePrivilegeGroup",

	"/v2/vectordb/indexes/list":             "DescribeIndex",
	"/v2/vectordb/indexes/describe":         "DescribeIndex",
	"/v2/vectordb/indexes/create":           "CreateIndex",
	"/v2/vectordb/indexes/drop":             "DropIndex",
	"/v2/vectordb/indexes/alter_properties": "AlterIndex",
	"/v2/vectordb/indexes/drop_properties":  "AlterIndex",

	"/v2/vectordb/aliases/list":     "ListAliases",
	"/v2/vectordb/aliases/describe": "DescribeAlias",
	"/v2/vectordb/aliases/create":   "CreateAlias",
	"/v2/vectordb/aliases/drop":     "DropAlias",
	"/v2/vectordb/aliases/alter":    "AlterAlias",

	"/v2/vectordb/jobs/import/list":                  "ListImports",
	"/v2/vectordb/jobs/import/create":                "Import",
	"/v2/vectordb/jobs/import/get_progress":          "GetImportProgress",
	"/v2/vectordb/jobs/import/describe":              "GetImportProgress",
	"/v2/vectordb/jobs/import/commit":                "CommitImport",
	"/v2/vectordb/jobs/import/abort":                 "AbortImport",
	"/v2/vectordb/jobs/snapshot/restore_external":    "RestoreExternalSnapshot",
	"/v2/vectordb/jobs/snapshot/export":              "ExportSnapshot",
	"/v2/vectordb/jobs/snapshot/describe":            "GetRestoreSnapshotState",
	"/v2/vectordb/jobs/snapshot/list":                "ListRestoreSnapshotJobs",
	"/v2/vectordb/jobs/external_collection/refresh":  "RefreshExternalCollection",
	"/v2/vectordb/jobs/external_collection/describe": "GetRefreshExternalCollectionProgress",
	"/v2/vectordb/jobs/external_collection/list":     "ListRefreshExternalCollectionJobs",

	"/v2/vectordb/resource_groups/create":           "CreateResourceGroup",
	"/v2/vectordb/resource_groups/drop":             "DropResourceGroup",
	"/v2/vectordb/resource_groups/alter":            "UpdateResourceGroups",
	"/v2/vectordb/resource_groups/describe":         "DescribeResourceGroup",
	"/v2/vectordb/resource_groups/list":             "ListResourceGroups",
	"/v2/vectordb/resource_groups/transfer_replica": "TransferMaster",

	"/v2/vectordb/file_resources/add":    "AddFileResource",
	"/v2/vectordb/file_resources/remove": "RemoveFileResource",
	"/v2/vectordb/file_resources/list":   "ListFileResources",

	"/v2/vectordb/segments/describe":    "GetSegmentsInfo",
	"/v2/vectordb/quotacenter/describe": "GetQuotaMetrics",

	"/v2/vectordb/common/run_analyzer": "RunAnalyzer",
}

func (h *HandlersV2) RegisterRoutesToV2(router gin.IRouter) {
	router.POST(CollectionCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listCollections))))
	router.POST(CollectionCategory+HasAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.hasCollection))))
	// todo review the return data
	router.POST(CollectionCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.getCollectionDetails))))
	router.POST(CollectionCategory+StatsAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.getCollectionStats))))
	router.POST(CollectionCategory+LoadStateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.getCollectionLoadState))))
	router.POST(CollectionCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionReq{AutoID: DisableAutoID} }, wrapperTraceLog(h.createCollection))))
	router.POST(CollectionCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.dropCollection))))
	router.POST(CollectionCategory+TruncateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.truncateCollection))))
	router.POST(CollectionCategory+RenameAction, timeoutMiddleware(wrapperPost(func() any { return &RenameCollectionReq{} }, wrapperTraceLog(h.renameCollection))))
	router.POST(CollectionCategory+LoadAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.loadCollection))))
	router.POST(CollectionCategory+RefreshLoadAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.refreshLoadCollection))))
	router.POST(CollectionCategory+ReleaseAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.releaseCollection))))
	router.POST(CollectionCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionReqWithProperties{} }, wrapperTraceLog(h.alterCollectionProperties))))
	router.POST(CollectionCategory+AddFunctionAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionAddFunction{} }, wrapperTraceLog(h.addCollectionFunction))))
	router.POST(CollectionCategory+AlterFunctionAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionAlterFunction{} }, wrapperTraceLog(h.alterCollectionFunction))))
	router.POST(CollectionCategory+DropFunctionAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionDropFunction{} }, wrapperTraceLog(h.dropCollectionFunction))))
	router.POST(CollectionCategory+AddFunctionFieldAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionAddFunctionField{} }, wrapperTraceLog(h.addCollectionFunctionField))))
	router.POST(CollectionCategory+DropFunctionFieldAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionDropFunctionField{} }, wrapperTraceLog(h.dropCollectionFunctionField))))
	router.POST(CollectionCategory+DropPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &DropCollectionPropertiesReq{} }, wrapperTraceLog(h.dropCollectionProperties))))
	router.POST(CollectionCategory+CompactAction, timeoutMiddleware(wrapperPost(func() any { return &CompactReq{} }, wrapperTraceLog(h.compact))))
	router.POST(CollectionCategory+CompactionStateAction, timeoutMiddleware(wrapperPost(func() any { return &GetCompactionStateReq{} }, wrapperTraceLog(h.getcompactionState))))
	router.POST(CollectionCategory+FlushAction, timeoutMiddleware(wrapperPost(func() any { return &FlushReq{} }, wrapperTraceLog(h.flush))))

	router.POST(CollectionFieldCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionFieldReqWithParams{} }, wrapperTraceLog(h.alterCollectionFieldProperties))))

	// /collections/fields/add
	router.POST(CollectionFieldCategory+AddAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionFieldReqWithSchema{} }, wrapperTraceLog(h.addCollectionField))))
	router.POST(CollectionFieldCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionDropField{} }, wrapperTraceLog(h.dropCollectionField))))
	router.POST(CollectionStructFieldCategory+AddAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionFieldReqWithSchema{} }, wrapperTraceLog(h.addCollectionStructField))))

	router.POST(DataBaseCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqWithProperties{} }, wrapperTraceLog(h.createDatabase))))
	router.POST(DataBaseCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqRequiredName{} }, wrapperTraceLog(h.dropDatabase))))
	router.POST(DataBaseCategory+DropPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &DropDatabasePropertiesReq{} }, wrapperTraceLog(h.dropDatabaseProperties))))
	router.POST(DataBaseCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &EmptyReq{} }, wrapperTraceLog(h.listDatabases))))
	router.POST(DataBaseCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqRequiredName{} }, wrapperTraceLog(h.describeDatabase))))
	router.POST(DataBaseCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqWithProperties{} }, wrapperTraceLog(h.alterDatabase))))
	router.POST(DataBaseCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqWithProperties{} }, wrapperTraceLog(h.alterDatabase))))
	// Query
	router.POST(EntityCategory+QueryAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &QueryReqV2{
			Limit:        100,
			OutputFields: []string{DefaultOutputFields},
		}
	}, wrapperTraceLog(h.query))), true))
	// Get
	router.POST(EntityCategory+GetAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionIDReq{
			OutputFields: []string{DefaultOutputFields},
		}
	}, wrapperTraceLog(h.get))), true))
	// Delete
	router.POST(EntityCategory+DeleteAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionFilterReq{}
	}, wrapperTraceLog(h.delete))), false))
	// Insert
	router.POST(EntityCategory+InsertAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionDataReq{}
	}, wrapperTraceLog(h.insert))), false))
	// Upsert
	router.POST(EntityCategory+UpsertAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionDataReq{}
	}, wrapperTraceLog(h.upsert))), false))
	// Search
	router.POST(EntityCategory+SearchAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &SearchReqV2{
			Limit: 100,
		}
	}, wrapperTraceLog(h.search))), true))
	// advanced_search, backward compatible uri
	router.POST(EntityCategory+AdvancedSearchAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &HybridSearchReq{
			Limit: 100,
		}
	}, wrapperTraceLog(h.advancedSearch))), true))
	// HybridSearch
	router.POST(EntityCategory+HybridSearchAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &HybridSearchReq{
			Limit: 100,
		}
	}, wrapperTraceLog(h.advancedSearch))), true))

	router.POST(PartitionCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.listPartitions))))
	router.POST(PartitionCategory+HasAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.hasPartitions))))
	router.POST(PartitionCategory+StatsAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.statsPartition))))

	router.POST(PartitionCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.createPartition))))
	router.POST(PartitionCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.dropPartition))))
	router.POST(PartitionCategory+LoadAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionsReq{} }, wrapperTraceLog(h.loadPartitions))))
	router.POST(PartitionCategory+ReleaseAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionsReq{} }, wrapperTraceLog(h.releasePartitions))))

	router.POST(UserCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listUsers))))
	router.POST(UserCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &UserReq{} }, wrapperTraceLog(h.describeUser))))

	router.POST(UserCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PasswordReq{} }, wrapperTraceLog(h.createUser))))
	router.POST(UserCategory+UpdatePasswordAction, timeoutMiddleware(wrapperPost(func() any { return &NewPasswordReq{} }, wrapperTraceLog(h.updateUser))))
	router.POST(UserCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &UserReq{} }, wrapperTraceLog(h.dropUser))))
	router.POST(UserCategory+GrantRoleAction, timeoutMiddleware(wrapperPost(func() any { return &UserRoleReq{} }, wrapperTraceLog(h.addRoleToUser))))
	router.POST(UserCategory+RevokeRoleAction, timeoutMiddleware(wrapperPost(func() any { return &UserRoleReq{} }, wrapperTraceLog(h.removeRoleFromUser))))

	router.POST(RoleCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listRoles))))
	router.POST(RoleCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.describeRole))))

	router.POST(RoleCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.createRole))))
	router.POST(RoleCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.alterRole))))
	router.POST(RoleCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.dropRole))))
	router.POST(RoleCategory+GrantPrivilegeAction, timeoutMiddleware(wrapperPost(func() any { return &GrantReq{} }, wrapperTraceLog(h.addPrivilegeToRole))))
	router.POST(RoleCategory+RevokePrivilegeAction, timeoutMiddleware(wrapperPost(func() any { return &GrantReq{} }, wrapperTraceLog(h.removePrivilegeFromRole))))
	router.POST(RoleCategory+GrantPrivilegeActionV2, timeoutMiddleware(wrapperPost(func() any { return &GrantV2Req{} }, wrapperTraceLog(h.grantV2))))
	router.POST(RoleCategory+RevokePrivilegeActionV2, timeoutMiddleware(wrapperPost(func() any { return &GrantV2Req{} }, wrapperTraceLog(h.revokeV2))))

	// privilege group
	router.POST(PrivilegeGroupCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.createPrivilegeGroup))))
	router.POST(PrivilegeGroupCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.dropPrivilegeGroup))))
	router.POST(PrivilegeGroupCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listPrivilegeGroups))))
	router.POST(PrivilegeGroupCategory+AddPrivilegesToGroupAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.addPrivilegesToGroup))))
	router.POST(PrivilegeGroupCategory+RemovePrivilegesFromGroupAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.removePrivilegesFromGroup))))

	router.POST(IndexCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.listIndexes))))
	router.POST(IndexCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReq{} }, wrapperTraceLog(h.describeIndex))))

	router.POST(IndexCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &IndexParamReq{} }, wrapperTraceLog(h.createIndex))))
	// todo cannot drop index before release it ?
	router.POST(IndexCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReq{} }, wrapperTraceLog(h.dropIndex))))
	router.POST(IndexCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReqWithProperties{} }, wrapperTraceLog(h.alterIndexProperties))))
	router.POST(IndexCategory+DropPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &DropIndexPropertiesReq{} }, wrapperTraceLog(h.dropIndexProperties))))

	router.POST(AliasCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &OptionalCollectionNameReq{} }, wrapperTraceLog(h.listAlias))))
	router.POST(AliasCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &AliasReq{} }, wrapperTraceLog(h.describeAlias))))

	router.POST(AliasCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &AliasCollectionReq{} }, wrapperTraceLog(h.createAlias))))
	router.POST(AliasCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &AliasReq{} }, wrapperTraceLog(h.dropAlias))))
	router.POST(AliasCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &AliasCollectionReq{} }, wrapperTraceLog(h.alterAlias))))

	router.POST(ImportJobCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &OptionalCollectionNameReq{} }, wrapperTraceLog(h.listImportJob))))
	router.POST(ImportJobCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &ImportReq{} }, wrapperTraceLog(h.createImportJob))))
	router.POST(ImportJobCategory+GetProgressAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.getImportJobProcess))))
	router.POST(ImportJobCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.getImportJobProcess))))
	router.POST(ImportJobCategory+CommitAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.commitImportJob))))
	router.POST(ImportJobCategory+AbortAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.abortImportJob))))
	router.POST(SnapshotJobCategory+RestoreExternalAction, timeoutMiddleware(wrapperPost(func() any { return &RestoreExternalSnapshotReq{} }, wrapperTraceLog(h.restoreExternalSnapshot))))
	router.POST(SnapshotJobCategory+ExportAction, timeoutMiddleware(wrapperPost(func() any { return &ExportSnapshotReq{} }, wrapperTraceLog(h.exportSnapshot))))
	router.POST(SnapshotJobCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.getRestoreSnapshotState))))
	router.POST(SnapshotJobCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &OptionalCollectionNameReq{} }, wrapperTraceLog(h.listRestoreSnapshotJobs))))
	router.POST(ExternalCollectionJobCategory+RefreshAction, timeoutMiddleware(wrapperPost(func() any { return &RefreshExternalCollectionReq{} }, wrapperTraceLog(h.refreshExternalCollection))))
	router.POST(ExternalCollectionJobCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &RefreshExternalCollectionProgressReq{} }, wrapperTraceLog(h.getRefreshExternalCollectionProgress))))
	router.POST(ExternalCollectionJobCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &OptionalCollectionNameReq{} }, wrapperTraceLog(h.listRefreshExternalCollectionJobs))))

	// resource group
	router.POST(ResourceGroupCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &ResourceGroupReq{} }, wrapperTraceLog(h.createResourceGroup))))
	router.POST(ResourceGroupCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &ResourceGroupReq{} }, wrapperTraceLog(h.dropResourceGroup))))
	router.POST(ResourceGroupCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &UpdateResourceGroupReq{} }, wrapperTraceLog(h.updateResourceGroup))))
	router.POST(ResourceGroupCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &ResourceGroupReq{} }, wrapperTraceLog(h.describeResourceGroup))))
	router.POST(ResourceGroupCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &EmptyReq{} }, wrapperTraceLog(h.listResourceGroups))))
	router.POST(ResourceGroupCategory+TransferReplicaAction, timeoutMiddleware(wrapperPost(func() any { return &TransferReplicaReq{} }, wrapperTraceLog(h.transferReplica))))

	// file resource
	router.POST(FileResourceCategory+AddAction, timeoutMiddleware(wrapperPost(func() any { return &FileResourceReq{} }, wrapperTraceLog(h.addFileResource))))
	router.POST(FileResourceCategory+RemoveAction, timeoutMiddleware(wrapperPost(func() any { return &FileResourceNameReq{} }, wrapperTraceLog(h.removeFileResource))))
	router.POST(FileResourceCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &EmptyReq{} }, wrapperTraceLog(h.listFileResources))))

	// segment group
	router.POST(SegmentCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &GetSegmentsInfoReq{} }, wrapperTraceLog(h.getSegmentsInfo))))
	router.POST(QuotaCenterCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &GetQuotaMetricsReq{} }, wrapperTraceLog(h.getQuotaMetrics))))

	// common
	router.POST(CommonCategory+RunAnalyzerAction, timeoutMiddleware(wrapperPost(func() any { return &RunAnalyzerReq{} }, wrapperTraceLog(h.runAnalyzer))))
}

type (
	newReqFunc    func() any
	handlerFuncV2 func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error)
)

func wrapperPost(newReq newReqFunc, v2 handlerFuncV2) gin.HandlerFunc {
	return func(gCtx *gin.Context) {
		req := newReq()
		if err := gCtx.ShouldBindBodyWith(req, binding.JSON); err != nil {
			mlog.Warn(context.TODO(), "high level restful api, read parameters from request body fail", mlog.Err(err),
				mlog.Any("url", gCtx.Request.URL.Path))
			if _, ok := err.(validator.ValidationErrors); ok {
				HTTPAbortReturn(gCtx, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
					HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", error: " + err.Error(),
				})
			} else if err == io.EOF {
				HTTPAbortReturn(gCtx, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", the request body should be nil, however {} is valid",
				})
			} else {
				HTTPAbortReturn(gCtx, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
				})
			}
			return
		}
		dbName := ""
		collectionName := ""
		if req != nil {
			if getter, ok := req.(requestutil.DBNameGetter); ok {
				dbName = getter.GetDbName()
			}
			if dbName == "" {
				dbName = gCtx.Request.Header.Get(HTTPHeaderDBName)
				if dbName == "" {
					dbName = DefaultDbName
				}
			}
			if getter, ok := req.(requestutil.CollectionNameGetter); ok {
				collectionName = getter.GetCollectionName()
			}
		}
		ctx := gCtx.Request.Context()
		username, _ := gCtx.Get(ContextUsername)
		ctx = proxy.NewContextWithMetadata(ctx, username.(string), dbName)
		mlog.Debug(ctx, "high level restful api, read parameters from request body, then start to handle.",
			mlog.Any("url", gCtx.Request.URL.Path))

		resp, err := v2(ctx, gCtx, req, dbName)
		methodTag, ok := routeToMethod[gCtx.FullPath()]
		if !ok {
			return
		}
		metrics.ProxyFunctionCall.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			methodTag,
			metrics.TotalLabel,
			metrics.CauseNA,
			dbName,
			collectionName,
		).Inc()
		label, cause := requestutil.ParseMetricLabel(resp, err)
		// set metrics for state code
		metrics.ProxyFunctionCall.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			methodTag,
			label,
			cause,
			dbName,
			collectionName,
		).Inc()

		// Mirror the metric's cause into the logs so a failed REST request can be
		// filtered by error_type. System failures are logged at Warn (actionable);
		// input failures at Info (expected user mistakes — keeping them at Warn
		// would spam the logs).
		if label == metrics.FailLabel && (cause == metrics.CauseSystem || cause == metrics.CauseUser) {
			status, _ := requestutil.GetStatusFromResponse(resp)
			errType := merr.SystemError
			if cause == metrics.CauseUser {
				errType = merr.InputError
			}
			logger := mlog.With(
				mlog.String("method", methodTag),
				mlog.String("error_type", errType.String()),
				mlog.Int32("code", status.GetCode()),
				mlog.String("reason", status.GetReason()),
			)
			if errType == merr.InputError {
				logger.Info(ctx, "restful request returned an input error")
			} else {
				logger.Warn(ctx, "restful request returned a system error")
			}
		}
	}
}

// restfulSizeMiddleware is the middleware fetchs metrics stats from gin struct.
func restfulSizeMiddleware(handler gin.HandlerFunc, observeOutbound bool) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		h := metrics.WrapRestfulContext(ctx.Request.Context(), ctx.Request.ContentLength)
		ctx.Request = ctx.Request.WithContext(h)
		handler(ctx)
		metrics.RecordRestfulMetrics(h, int64(ctx.Writer.Size()), observeOutbound)
	}
}

func getTraceLogRequestFieldWithoutSensitiveInfo(req any) mlog.Field {
	switch request := req.(type) {
	case *RestoreExternalSnapshotReq:
		if request == nil {
			return proxy.GetRequestFieldWithoutSensitiveInfo(req)
		}
		redactedReq := *request
		redactedReq.ExternalSpec = externalspec.RedactExternalSpec(request.ExternalSpec)
		return mlog.Any("request", &redactedReq)
	case *ExportSnapshotReq:
		if request == nil {
			return proxy.GetRequestFieldWithoutSensitiveInfo(req)
		}
		redactedReq := *request
		redactedReq.ExternalSpec = externalspec.RedactExternalSpec(request.ExternalSpec)
		return mlog.Any("request", &redactedReq)
	default:
		return proxy.GetRequestFieldWithoutSensitiveInfo(req)
	}
}

func wrapperTraceLog(v2 handlerFuncV2) handlerFuncV2 {
	return func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		switch proxy.Params.CommonCfg.TraceLogMode.GetAsInt() {
		case 1: // simple info
			fields := proxy.GetRequestBaseInfo(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: c.Request.URL.Path,
			}, false)
			mlog.Info(ctx, "trace info: simple", fields...)
		case 2: // detail info
			fields := proxy.GetRequestBaseInfo(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: c.Request.URL.Path,
			}, true)
			fields = append(fields, getTraceLogRequestFieldWithoutSensitiveInfo(req))
			mlog.Info(ctx, "trace info: detail", fields...)
		case 3: // detail info with request and response
			fields := proxy.GetRequestBaseInfo(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: c.Request.URL.Path,
			}, true)
			fields = append(fields, getTraceLogRequestFieldWithoutSensitiveInfo(req))
			mlog.Info(ctx, "trace info: all request", fields...)
		}
		resp, err := v2(ctx, c, req, dbName)
		if proxy.Params.CommonCfg.TraceLogMode.GetAsInt() > 2 {
			if err != nil {
				mlog.Info(ctx, "trace info: all, error", mlog.Err(err))
			} else {
				mlog.Info(ctx, "trace info: all, unknown")
			}
		}
		return resp, err
	}
}

func checkAuthorizationV2(ctx context.Context, c *gin.Context, ignoreErr bool, req interface{}) error {
	username, ok := c.Get(ContextUsername)
	if !ok || username.(string) == "" {
		if !ignoreErr {
			HTTPReturn(c, http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
		}
		hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(merr.ErrNeedAuthenticate), nil, c.FullPath(), hookutil.ActionAuthorize)
		return merr.ErrNeedAuthenticate
	}
	ctx, authErr := proxy.PrivilegeInterceptor(ctx, req)
	if authErr != nil {
		if !ignoreErr {
			HTTPReturn(c, http.StatusForbidden, gin.H{HTTPReturnCode: merr.Code(authErr), HTTPReturnMessage: authErr.Error()})
		}
		hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(authErr), nil, c.FullPath(), hookutil.ActionAuthorize)
		return authErr
	}

	c.Request = c.Request.WithContext(ctx)
	return nil
}

func checkAuthorizationHelper(ctx context.Context, c *gin.Context, req interface{}) error {
	username, ok := c.Get(ContextUsername)
	if !ok || username.(string) == "" {
		return merr.ErrNeedAuthenticate
	}
	ctx, authErr := proxy.PrivilegeInterceptor(ctx, req)
	if authErr != nil {
		return authErr
	}
	c.Request = c.Request.WithContext(ctx)
	return nil
}

func wrapperProxy(ctx context.Context, c *gin.Context, req any, checkAuth bool, ignoreErr bool, fullMethod string, handler func(reqCtx context.Context, req any) (any, error)) (interface{}, error) {
	return wrapperProxyWithLimit(ctx, c, req, checkAuth, ignoreErr, fullMethod, false, nil, handler)
}

func wrapperProxyWithLimit(ctx context.Context, ginCtx *gin.Context, req any, checkAuth bool, ignoreErr bool, fullMethod string, checkLimit bool, pxy types.ProxyComponent, handler func(reqCtx context.Context, req any) (any, error)) (interface{}, error) {
	if baseGetter, ok := req.(BaseGetter); ok {
		span := trace.SpanFromContext(ctx)
		span.AddEvent(baseGetter.GetBase().GetMsgType().String())
	}
	if checkAuth {
		err := checkAuthorizationV2(ctx, ginCtx, ignoreErr, req)
		if err != nil {
			return nil, err
		}
		ctx = ginCtx.Request.Context()
	}
	if checkLimit {
		_, err := CheckLimiter(ctx, req, pxy)
		if err != nil {
			mlog.Warn(context.TODO(), "high level restful api, fail to check limiter", mlog.Err(err), mlog.String("method", fullMethod))
			hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(merr.ErrHTTPRateLimit), nil, ginCtx.FullPath(), hookutil.ActionAuthorize)
			HTTPAbortReturn(ginCtx, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrHTTPRateLimit),
				HTTPReturnMessage: merr.ErrHTTPRateLimit.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
	}
	mlog.Debug(ctx, "high level restful api, try to do a grpc call")
	username, ok := ginCtx.Get(ContextUsername)
	if !ok {
		username = ""
	}

	forwardHandler := func(reqCtx context.Context, req any) (any, error) {
		interceptor := streaming.ForwardLegacyProxyUnaryServerInterceptor()
		if token, ok := ginCtx.Get(ContextToken); ok {
			interceptor = streaming.ForwardLegacyProxyUnaryServerInterceptor(streaming.OptForwardAuth(token.(string)))
		}
		return interceptor(reqCtx, req, &grpc.UnaryServerInfo{FullMethod: fullMethod}, func(ctx context.Context, req any) (interface{}, error) {
			return handler(ctx, req)
		})
	}
	response, err := proxy.HookInterceptor(context.WithValue(ctx, hook.GinParamsKey, ginCtx), req, username.(string), fullMethod, forwardHandler)
	if response != nil {
		ginCtx.Set(ContextResponse, response)
	}
	if err == nil {
		status, ok := requestutil.GetStatusFromResponse(response)
		if ok {
			err = merr.Error(status)
		}
	}

	if err != nil {
		// Expose the exact classification (incl. boundary InputError marks) to
		// the REST access log; key must match accesslog/info.ContextErrorType.
		ginCtx.Set("error_type", merr.GetErrorType(err).String())
		mlog.Warn(ctx, "high level restful api, grpc call failed", mlog.Err(err))
		if !ignoreErr {
			HTTPAbortReturn(ginCtx, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		}
	}
	return response, err
}

func (h *HandlersV2) hasCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	collectionName := getter.GetCollectionName()
	_, err := proxy.GetCachedCollectionSchema(ctx, dbName, collectionName)
	has := true
	if err != nil {
		req := &milvuspb.HasCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		}
		// handle at rootcoord side
		resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/HasCollection", func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.HasCollection(reqCtx, req.(*milvuspb.HasCollectionRequest))
		})
		if err != nil {
			return nil, err
		}
		has = resp.(*milvuspb.BoolResponse).Value
	}
	HTTPReturn(c, http.StatusOK, wrapperReturnHas(has))
	return has, nil
}

func (h *HandlersV2) listCollections(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ShowCollectionsRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	// handle at rootcoord side
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ShowCollections", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ShowCollections(reqCtx, req.(*milvuspb.ShowCollectionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ShowCollectionsResponse).CollectionNames))
	}
	return resp, err
}

func (h *HandlersV2) getCollectionDetails(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	collectionName := collectionGetter.GetCollectionName()
	req := &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeCollection", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})
	if err != nil {
		return resp, err
	}
	coll := resp.(*milvuspb.DescribeCollectionResponse)
	primaryField, ok := getPrimaryField(coll.Schema)
	autoID := false
	if !ok {
		mlog.Warn(ctx, "high level restful api, get primary field from collection schema fail", mlog.Any("collection schema", coll.Schema), mlog.Any("request", anyReq))
	} else {
		autoID = primaryField.AutoID
	}
	errMessage := ""
	loadStateReq := &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	stateResp, err := wrapperProxy(ctx, c, loadStateReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/GetLoadState", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadState(reqCtx, req.(*milvuspb.GetLoadStateRequest))
	})
	collLoadState := ""
	if err == nil {
		collLoadState = stateResp.(*milvuspb.GetLoadStateResponse).State.String()
	} else {
		errMessage += err.Error() + ";"
	}
	vectorField := ""
	for _, field := range coll.Schema.Fields {
		if typeutil.IsVectorType(field.DataType) {
			vectorField = field.Name
			break
		}
	}
	indexDesc := []gin.H{}
	descIndexReq := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      vectorField,
	}
	indexResp, err := wrapperProxy(ctx, c, descIndexReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/DescribeIndex", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err == nil {
		indexDesc = printIndexes(indexResp.(*milvuspb.DescribeIndexResponse).IndexDescriptions)
	} else {
		errMessage += err.Error() + ";"
	}
	var aliases []string
	aliasReq := &milvuspb.ListAliasesRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	aliasResp, err := wrapperProxy(ctx, c, aliasReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/ListAliases", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListAliases(reqCtx, req.(*milvuspb.ListAliasesRequest))
	})
	if err == nil {
		aliases = aliasResp.(*milvuspb.ListAliasesResponse).GetAliases()
	} else {
		errMessage += err.Error() + "."
	}
	if aliases == nil {
		aliases = []string{}
	}
	if coll.Properties == nil {
		coll.Properties = []*commonpb.KeyValuePair{}
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
		HTTPCollectionName:    coll.CollectionName,
		HTTPCollectionID:      coll.CollectionID,
		HTTPReturnDescription: coll.Schema.Description,
		HTTPReturnFieldAutoID: autoID,
		"fields":              printFieldsV2(coll.Schema.Fields),
		"structFields":        printStructArrayFieldsV2(coll.Schema.StructArrayFields),
		"functions":           printFunctionDetails(coll.Schema.Functions),
		"aliases":             aliases,
		"indexes":             indexDesc,
		"load":                collLoadState,
		"shardsNum":           coll.ShardsNum,
		"partitionsNum":       coll.NumPartitions,
		"consistencyLevel":    commonpb.ConsistencyLevel_name[int32(coll.ConsistencyLevel)],
		"enableDynamicField":  coll.Schema.EnableDynamicField,
		"externalSource":      coll.Schema.GetExternalSource(),
		"externalSpec":        coll.Schema.GetExternalSpec(),
		"properties":          coll.Properties,
	}, HTTPReturnMessage: errMessage})
	return resp, nil
}

func (h *HandlersV2) getCollectionStats(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.GetCollectionStatisticsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetCollectionStatistics", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetCollectionStatistics(reqCtx, req.(*milvuspb.GetCollectionStatisticsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnRowCount(resp.(*milvuspb.GetCollectionStatisticsResponse).Stats))
	}
	return resp, err
}

func (h *HandlersV2) getCollectionLoadState(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetLoadState", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadState(reqCtx, req.(*milvuspb.GetLoadStateRequest))
	})
	if err != nil {
		return resp, err
	}
	switch resp.(*milvuspb.GetLoadStateResponse).State {
	case commonpb.LoadState_LoadStateNotExist:
		err = merr.WrapErrCollectionNotFound(req.CollectionName)
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return resp, err
	case commonpb.LoadState_LoadStateNotLoad:
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
			HTTPReturnLoadState: resp.(*milvuspb.GetLoadStateResponse).State.String(),
		}})
		return resp, err
	}
	partitionsGetter, _ := anyReq.(requestutil.PartitionNamesGetter)
	progressReq := &milvuspb.GetLoadingProgressRequest{
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionNames: partitionsGetter.GetPartitionNames(),
		DbName:         dbName,
	}
	progressResp, err := wrapperProxy(ctx, c, progressReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/GetLoadingProgress", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadingProgress(reqCtx, req.(*milvuspb.GetLoadingProgressRequest))
	})
	progress := int64(-1)
	errMessage := ""
	if err == nil {
		progress = progressResp.(*milvuspb.GetLoadingProgressResponse).Progress
	} else {
		errMessage += err.Error() + "."
	}
	state := commonpb.LoadState_LoadStateLoading.String()
	if progress >= 100 {
		state = commonpb.LoadState_LoadStateLoaded.String()
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
		HTTPReturnLoadState:    state,
		HTTPReturnLoadProgress: progress,
	}, HTTPReturnMessage: errMessage})
	return resp, err
}

func (h *HandlersV2) dropCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropCollection(reqCtx, req.(*milvuspb.DropCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) truncateCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.TruncateCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/TruncateCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.TruncateCollection(reqCtx, req.(*milvuspb.TruncateCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) renameCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RenameCollectionReq)
	req := &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   httpReq.CollectionName,
		NewName:   httpReq.NewCollectionName,
		NewDBName: httpReq.NewDbName,
	}
	c.Set(ContextRequest, req)
	if req.NewDBName == "" {
		req.NewDBName = dbName
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RenameCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RenameCollection(reqCtx, req.(*milvuspb.RenameCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) refreshLoadCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
		Refresh:        true,
	}
	return h.loadCollectionInternal(ctx, c, req, dbName)
}

func (h *HandlersV2) loadCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	return h.loadCollectionInternal(ctx, c, req, dbName)
}

func (h *HandlersV2) loadCollectionInternal(ctx context.Context, c *gin.Context, req *milvuspb.LoadCollectionRequest, dbName string) (interface{}, error) {
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/LoadCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadCollection(reqCtx, req.(*milvuspb.LoadCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) releaseCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ReleaseCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ReleaseCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ReleaseCollection(reqCtx, req.(*milvuspb.ReleaseCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterCollectionProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionReqWithProperties)
	req := &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollection(reqCtx, req.(*milvuspb.AlterCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) addCollectionFunction(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionAddFunction)
	req := &milvuspb.AddCollectionFunctionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	fSchema, err := genFunctionSchema(ctx, &httpReq.Function)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}

	req.FunctionSchema = fSchema
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AddCollectionFunction", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AddCollectionFunction(reqCtx, req.(*milvuspb.AddCollectionFunctionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterCollectionFunction(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionAlterFunction)
	req := &milvuspb.AlterCollectionFunctionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		FunctionName:   httpReq.FunctionName,
	}
	fSchema, err := genFunctionSchema(ctx, &httpReq.Function)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}

	req.FunctionSchema = fSchema
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionFunction", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollectionFunction(reqCtx, req.(*milvuspb.AlterCollectionFunctionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// dropCollectionFunction backs the deprecated /collections/drop_function endpoint,
// which mapped to the legacy detach RPC. A function is coupled to its output field,
// so the RPC is rejected; use /collections/drop_function_field instead.
func (h *HandlersV2) dropCollectionFunction(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDropFunction)
	req := &milvuspb.DropCollectionFunctionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		FunctionName:   httpReq.FunctionName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropCollectionFunction", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropCollectionFunction(reqCtx, req.(*milvuspb.DropCollectionFunctionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// addCollectionFunctionField backs /collections/add_function_field: it attaches a
// BM25/MinHash function together with its output field and index via AlterCollectionSchema.
// A function is coupled to its output field, so both are added in one request.
func (h *HandlersV2) addCollectionFunctionField(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionAddFunctionField)
	// The index always targets the newly-added output field; reject a mismatched
	// indexParams.fieldName rather than silently building the index on outputField.
	if httpReq.IndexParam.FieldName != httpReq.OutputField.FieldName {
		err := merr.WrapErrParameterInvalidMsg(
			"indexParams.fieldName %q must match outputField.fieldName %q",
			httpReq.IndexParam.FieldName, httpReq.OutputField.FieldName)
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	fSchema, err := genFunctionSchema(ctx, &httpReq.Function)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	fieldSchema, err := httpReq.OutputField.GetProto(ctx)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	extraParams, err := convertToExtraParams(httpReq.IndexParam)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{
							FieldSchema: fieldSchema,
							IndexName:   httpReq.IndexParam.IndexName,
							ExtraParams: extraParams,
						},
					},
					FuncSchema: []*schemapb.FunctionSchema{fSchema},
				},
			},
		},
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionSchema", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		r, callErr := h.proxy.AlterCollectionSchema(reqCtx, req.(*milvuspb.AlterCollectionSchemaRequest))
		if callErr == nil {
			callErr = merr.Error(r.GetAlterStatus())
		}
		return r, callErr
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// dropCollectionFunctionField backs /collections/drop_function_field: it detaches a
// function and drops its output field together via AlterCollectionSchema.
func (h *HandlersV2) dropCollectionFunctionField(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDropFunctionField)
	req := &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FunctionName{
						FunctionName: httpReq.FunctionName,
					},
					DropFunctionOutputFields: true,
				},
			},
		},
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionSchema", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		r, callErr := h.proxy.AlterCollectionSchema(reqCtx, req.(*milvuspb.AlterCollectionSchemaRequest))
		if callErr == nil {
			callErr = merr.Error(r.GetAlterStatus())
		}
		return r, callErr
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropCollectionProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DropCollectionPropertiesReq)
	req := &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		DeleteKeys:     httpReq.PropertyKeys,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollection(reqCtx, req.(*milvuspb.AlterCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) compact(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CompactReq)
	req := &milvuspb.ManualCompactionRequest{
		DbName:          dbName,
		CollectionName:  httpReq.CollectionName,
		MajorCompaction: httpReq.IsClustering,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ManualCompaction", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ManualCompaction(reqCtx, req.(*milvuspb.ManualCompactionRequest))
	})
	if err == nil {
		resp := resp.(*milvuspb.ManualCompactionResponse)
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"compactionID": resp.CompactionID},
		})
	}
	return resp, err
}

func (h *HandlersV2) getcompactionState(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*GetCompactionStateReq)
	req := &milvuspb.GetCompactionStateRequest{
		CompactionID: httpReq.JobID,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetCompactionState", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetCompactionState(reqCtx, req.(*milvuspb.GetCompactionStateRequest))
	})
	if err == nil {
		resp := resp.(*milvuspb.GetCompactionStateResponse)
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"compactionID": httpReq.JobID, "state": resp.State.String(), "executingPlanNumber": resp.ExecutingPlanNo, "timeoutPlanNumber": resp.TimeoutPlanNo, "completedPlanNumber": resp.CompletedPlanNo},
		})
	}
	return resp, err
}

func (h *HandlersV2) flush(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*FlushReq)
	req := &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{httpReq.CollectionName},
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Flush", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Flush(reqCtx, req.(*milvuspb.FlushRequest))
	})
	if err == nil {
		err = h.waitForFlush(ctx, dbName, httpReq.CollectionName, resp.(*milvuspb.FlushResponse))
		if err != nil {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return resp, err
		}
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) waitForFlush(ctx context.Context, dbName string, collectionName string, flushResp *milvuspb.FlushResponse) error {
	segmentIDs := flushResp.GetCollSegIDs()[collectionName].GetData()
	flushTs, ok := flushResp.GetCollFlushTs()[collectionName]
	if !ok {
		return merr.WrapErrServiceInternalMsg("failed to get flush timestamp for collection %s", collectionName)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		stateResp, err := h.proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			SegmentIDs:     segmentIDs,
			FlushTs:        flushTs,
		})
		if err := merr.CheckRPCCall(stateResp, err); err != nil {
			return err
		}
		if stateResp.GetFlushed() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *HandlersV2) alterCollectionFieldProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionFieldReqWithParams)
	req := &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		FieldName:      httpReq.FieldName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.FieldParams))
	for key, value := range httpReq.FieldParams {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionField", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollectionField(reqCtx, req.(*milvuspb.AlterCollectionFieldRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) addCollectionField(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionFieldReqWithSchema)

	if httpReq.Schema.IsStructArrayField() {
		err := merr.WrapErrParameterInvalidMsg("StructArray field must be added through /v2/vectordb/collections/struct_fields/add")
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}

	schemaProto, err := httpReq.Schema.GetProto(ctx)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}

	req := &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: schemaProto},
					},
				},
			},
		},
	}

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionSchema", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		r, callErr := h.proxy.AlterCollectionSchema(reqCtx, req.(*milvuspb.AlterCollectionSchemaRequest))
		if callErr == nil {
			callErr = merr.Error(r.GetAlterStatus())
		}
		return r, callErr
	})

	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropCollectionField(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDropField)
	hasFieldName := httpReq.FieldName != ""
	hasFieldID := httpReq.FieldID != nil
	if hasFieldName == hasFieldID {
		err := merr.WrapErrParameterInvalidMsg("exactly one of fieldName or fieldId is required")
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}
	if hasFieldID && *httpReq.FieldID <= 0 {
		err := merr.WrapErrParameterInvalidMsg("fieldId must be greater than 0")
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}

	dropRequest := &milvuspb.AlterCollectionSchemaRequest_DropRequest{}
	if hasFieldName {
		dropRequest.Identifier = &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: httpReq.FieldName}
	} else {
		dropRequest.Identifier = &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldId{FieldId: *httpReq.FieldID}
	}
	req := &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{DropRequest: dropRequest},
		},
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionSchema", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		r, callErr := h.proxy.AlterCollectionSchema(reqCtx, req.(*milvuspb.AlterCollectionSchemaRequest))
		if callErr == nil {
			callErr = merr.Error(r.GetAlterStatus())
		}
		return r, callErr
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) addCollectionStructField(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionFieldReqWithSchema)

	schemaProto, err := httpReq.Schema.GetStructArrayProto(ctx)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}

	req := &milvuspb.AddCollectionStructFieldRequest{
		DbName:                 dbName,
		CollectionName:         httpReq.CollectionName,
		StructArrayFieldSchema: schemaProto,
	}

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AddCollectionStructField", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AddCollectionStructField(reqCtx, req.(*milvuspb.AddCollectionStructFieldRequest))
	})

	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// copy from internal/proxy/task_query.go
func matchCountRule(outputs []string) bool {
	return len(outputs) == 1 && strings.ToLower(strings.TrimSpace(outputs[0])) == "count(*)"
}

func (h *HandlersV2) query(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*QueryReqV2)
	req := &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Expr:           httpReq.Filter,
		OutputFields:   httpReq.OutputFields,
		PartitionNames: httpReq.PartitionNames,
		QueryParams:    []*commonpb.KeyValuePair{},
	}
	var err error
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	req.ConsistencyLevel, req.UseDefaultConsistency, err = convertConsistencyLevel(httpReq.ConsistencyLevel)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, query with consistency_level invalid", mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:" + err.Error(),
		})
		return nil, err
	}
	req.ExprTemplateValues = generateExpressionTemplate(httpReq.ExprParams)
	c.Set(ContextRequest, req)
	if httpReq.Offset > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: proxy.OffsetKey, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	}
	if httpReq.Limit > 0 && !matchCountRule(httpReq.OutputFields) {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: proxy.LimitKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	}
	if len(httpReq.OrderByFields) > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: proxy.OrderByFieldsKey, Value: strings.Join(httpReq.OrderByFields, ",")})
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Query", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == nil {
		queryResp := resp.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS, collSchema)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, fail to deal with query result", mlog.Any("response", resp), mlog.Err(err))
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			scannedRemoteBytes, scannedTotalBytes, cacheHitRatio, isValid := proxy.GetStorageCost(queryResp.GetStatus())
			if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
				HTTPReturnStream(c, http.StatusOK, gin.H{
					HTTPReturnCode:               merr.Code(nil),
					HTTPReturnData:               outputData,
					HTTPReturnCost:               proxy.GetCostValue(queryResp.GetStatus()),
					HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
					HTTPReturnScannedTotalBytes:  scannedTotalBytes,
					HTTPReturnCacheHitRatio:      cacheHitRatio,
				})
			} else {
				HTTPReturnStream(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: outputData,
					HTTPReturnCost: proxy.GetCostValue(queryResp.GetStatus()),
				})
			}
		}
	}
	return resp, err
}

func (h *HandlersV2) get(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionIDReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
			HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		OutputFields:   httpReq.OutputFields,
		PartitionNames: httpReq.PartitionNames,
		Expr:           filter,
	}
	req.ConsistencyLevel, req.UseDefaultConsistency, err = convertConsistencyLevel(httpReq.ConsistencyLevel)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, query with consistency_level invalid", mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:" + err.Error(),
		})
		return nil, err
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Query", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == nil {
		queryResp := resp.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS, collSchema)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, fail to deal with get result", mlog.Any("response", resp), mlog.Err(err))
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			scannedRemoteBytes, scannedTotalBytes, cacheHitRatio, isValid := proxy.GetStorageCost(queryResp.GetStatus())
			if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
				HTTPReturnStream(c, http.StatusOK, gin.H{
					HTTPReturnCode:               merr.Code(nil),
					HTTPReturnData:               outputData,
					HTTPReturnCost:               proxy.GetCostValue(queryResp.GetStatus()),
					HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
					HTTPReturnScannedTotalBytes:  scannedTotalBytes,
					HTTPReturnCacheHitRatio:      cacheHitRatio,
				})
			} else {
				HTTPReturnStream(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: outputData,
					HTTPReturnCost: proxy.GetCostValue(queryResp.GetStatus()),
				})
			}
		}
	}
	return resp, err
}

func (h *HandlersV2) delete(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionFilterReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	req := &milvuspb.DeleteRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		Expr:           httpReq.Filter,
	}
	req.ExprTemplateValues = generateExpressionTemplate(httpReq.ExprParams)
	c.Set(ContextRequest, req)
	if req.Expr == "" {
		body, _ := c.Get(gin.BodyBytesKey)
		filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
		if err != nil {
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		req.Expr = filter
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Delete", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Delete(reqCtx, req.(*milvuspb.DeleteRequest))
	})
	if err == nil {
		deleteResp := resp.(*milvuspb.MutationResult)
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"deleteCount": deleteResp.DeleteCnt},
		})
	}
	return resp, err
}

func (h *HandlersV2) insert(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDataReq)
	req := &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		// PartitionName:  "_default",
	}
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	var validDataMap map[string][]bool
	httpReq.Data, validDataMap, err = checkAndSetData(body.([]byte), collSchema, false)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, fail to deal with insert data", mlog.Err(err), mlog.String("body", string(body.([]byte))))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}

	req.NumRows = uint32(len(httpReq.Data))
	req.FieldsData, err = anyToColumns(httpReq.Data, validDataMap, collSchema, true, false)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, fail to deal with insert data", mlog.Any("data", httpReq.Data), mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Insert", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Insert(reqCtx, req.(*milvuspb.InsertRequest))
	})
	if err == nil {
		insertResp := resp.(*milvuspb.MutationResult)
		cost := proxy.GetCostValue(insertResp.GetStatus())
		switch insertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data},
					HTTPReturnCost: cost,
				})
			} else {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": formatInt64(insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)},
					HTTPReturnCost: cost,
				})
			}
		case *schemapb.IDs_StrId:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode: merr.Code(nil),
				HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data},
				HTTPReturnCost: cost,
			})
		default:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
	return resp, err
}

func (h *HandlersV2) upsert(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDataReq)
	req := &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		PartialUpdate:  httpReq.PartialUpdate,
		// PartitionName:  "_default",
	}
	fieldOps, err := buildFieldPartialUpdateOps(httpReq.FieldOps)
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	req.FieldOps = fieldOps
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	var validDataMap map[string][]bool
	httpReq.Data, validDataMap, err = checkAndSetData(body.([]byte), collSchema, httpReq.PartialUpdate)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, fail to deal with upsert data", mlog.Any("body", body), mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}

	req.NumRows = uint32(len(httpReq.Data))
	req.FieldsData, err = anyToColumns(httpReq.Data, validDataMap, collSchema, false, httpReq.PartialUpdate)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, fail to deal with upsert data", mlog.Any("data", httpReq.Data), mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Upsert", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Upsert(reqCtx, req.(*milvuspb.UpsertRequest))
	})
	if err == nil {
		upsertResp := resp.(*milvuspb.MutationResult)
		cost := proxy.GetCostValue(upsertResp.GetStatus())
		scannedRemoteBytes, scannedTotalBytes, cacheHitRatio, isValid := proxy.GetStorageCost(upsertResp.GetStatus())
		switch upsertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
					HTTPReturn(c, http.StatusOK, gin.H{
						HTTPReturnCode:               merr.Code(nil),
						HTTPReturnData:               gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data},
						HTTPReturnCost:               cost,
						HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
						HTTPReturnScannedTotalBytes:  scannedTotalBytes,
						HTTPReturnCacheHitRatio:      cacheHitRatio,
					})
				} else {
					HTTPReturn(c, http.StatusOK, gin.H{
						HTTPReturnCode: merr.Code(nil),
						HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data},
						HTTPReturnCost: cost,
					})
				}
			} else {
				if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
					HTTPReturn(c, http.StatusOK, gin.H{
						HTTPReturnCode:               merr.Code(nil),
						HTTPReturnData:               gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": formatInt64(upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)},
						HTTPReturnCost:               cost,
						HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
						HTTPReturnScannedTotalBytes:  scannedTotalBytes,
						HTTPReturnCacheHitRatio:      cacheHitRatio,
					})
				} else {
					HTTPReturn(c, http.StatusOK, gin.H{
						HTTPReturnCode: merr.Code(nil),
						HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": formatInt64(upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)},
						HTTPReturnCost: cost,
					})
				}
			}
		case *schemapb.IDs_StrId:
			if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:               merr.Code(nil),
					HTTPReturnData:               gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data},
					HTTPReturnCost:               cost,
					HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
					HTTPReturnScannedTotalBytes:  scannedTotalBytes,
					HTTPReturnCacheHitRatio:      cacheHitRatio,
				})
			} else {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data},
					HTTPReturnCost: cost,
				})
			}
		default:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
	return resp, err
}

func generatePlaceholderGroup(ctx context.Context, body string, collSchema *schemapb.CollectionSchema, fieldName string) ([]byte, error) {
	var err error
	var vectorField *schemapb.FieldSchema
	if len(fieldName) == 0 {
		for _, field := range collSchema.Fields {
			if typeutil.IsVectorType(field.DataType) {
				if len(fieldName) == 0 {
					fieldName = field.Name
					vectorField = field
				} else {
					return nil, merr.WrapErrParameterInvalidMsg("search without annsField, but already found multiple vector fields: [%s, %s,,,]", fieldName, field.Name)
				}
			}
		}
	} else {
		for _, field := range collSchema.Fields {
			if field.Name == fieldName && typeutil.IsVectorType(field.DataType) {
				vectorField = field
				break
			}
		}
		if vectorField == nil {
			for _, sf := range collSchema.GetStructArrayFields() {
				for _, sub := range sf.GetFields() {
					if sub.GetName() == fieldName && typeutil.IsVectorType(sub.GetDataType()) {
						vectorField = sub
						break
					}
				}
				if vectorField != nil {
					break
				}
			}
		}
	}
	if vectorField == nil {
		return nil, merr.WrapErrFieldNotFound(fieldName, "cannot find a vector field")
	}
	dim := int64(0)
	if !typeutil.IsSparseFloatVectorType(vectorField.DataType) {
		dim, _ = getDim(vectorField)
	}

	dataType := vectorField.DataType

	if vectorField.GetIsFunctionOutput() {
		for _, function := range collSchema.Functions {
			if function.Type == schemapb.FunctionType_BM25 || function.Type == schemapb.FunctionType_TextEmbedding || function.Type == schemapb.FunctionType_MinHash {
				// TODO: currently only BM25, text & MinHash embedding function is supported, thus guarantees one input field to one output field
				if function.OutputFieldNames[0] == vectorField.Name {
					dataType = schemapb.DataType_VarChar
				}
			}
		}
	}

	var phv *commonpb.PlaceholderValue
	if vectorField.GetDataType() == schemapb.DataType_ArrayOfVector {
		if isEmbeddingListData(body) {
			phv, err = convertEmbListQueries2Placeholder(body, vectorField.GetElementType(), dim)
		} else {
			phv, err = convertQueries2Placeholder(body, vectorField.GetElementType(), dim)
		}
	} else {
		phv, err = convertQueries2Placeholder(body, dataType, dim)
	}
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			phv,
		},
	})
}

func (h *HandlersV2) search(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*SearchReqV2)
	req := &milvuspb.SearchRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Dsl:            httpReq.Filter,
		DslType:        commonpb.DslType_BoolExprV1,
		OutputFields:   httpReq.OutputFields,
		PartitionNames: httpReq.PartitionNames,
	}
	var err error
	req.ConsistencyLevel, req.UseDefaultConsistency, err = convertConsistencyLevel(httpReq.ConsistencyLevel)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, search with consistency_level invalid", mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:" + err.Error(),
		})
		return nil, err
	}
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		// has already throw http in GetCollectionSchema if fails to get schema
		return nil, err
	}

	// Check if search by primary keys or by vectors
	hasIDs := len(httpReq.Ids) > 0
	hasData := len(httpReq.Data) > 0

	// Primary keys and query vectors are mutually exclusive
	if hasIDs && hasData {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
			HTTPReturnMessage: "primary keys (ids) and query vectors (data) are mutually exclusive. Please provide either 'ids' or 'data', not both",
		})
		return nil, merr.ErrParameterInvalid
	}

	// At least one of ids or data must be provided
	if !hasIDs && !hasData {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: "either 'ids' (for primary key search) or 'data' (for vector search) must be provided",
		})
		return nil, merr.ErrMissingRequiredParameters
	}

	if httpReq.SearchAggregation != nil {
		if hasIDs {
			err := merr.WrapErrParameterInvalidMsg("ids and searchAggregation cannot be used simultaneously")
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
		if httpReq.Offset != 0 {
			err := merr.WrapErrParameterInvalidMsg("offset is not supported with searchAggregation")
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
		if searchParamsContainAny(httpReq.SearchParams, proxy.OffsetKey) {
			err := merr.WrapErrParameterInvalidMsg("searchParams.offset is not supported with searchAggregation")
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
		if httpReq.GroupByField != "" || httpReq.GroupSize != 0 || httpReq.StrictGroupSize {
			err := merr.WrapErrParameterInvalidMsg("groupingField/groupSize/strictGroupSize and searchAggregation cannot be used simultaneously")
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
		if searchParamsContainAny(httpReq.SearchParams, proxy.GroupByFieldKey, proxy.GroupByFieldsKey) {
			err := merr.WrapErrParameterInvalidMsg("searchParams.group_by_field(s) and searchAggregation cannot be used simultaneously")
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
		req.SearchAggregation, err = convertSearchAggregationReq(httpReq.SearchAggregation)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, convert SearchAggregation failed", mlog.Err(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
	}

	searchParams, err := generateSearchParams(httpReq.SearchParams)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, generate SearchParams failed", mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: common.TopKKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.OffsetKey, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	if httpReq.GroupByField != "" {
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamGroupByField, Value: httpReq.GroupByField})
	}
	if httpReq.GroupByField != "" && httpReq.GroupSize > 0 {
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamGroupSize, Value: strconv.FormatInt(int64(httpReq.GroupSize), 10)})
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamStrictGroupSize, Value: strconv.FormatBool(httpReq.StrictGroupSize)})
	}
	if len(httpReq.FunctionScore.Functions) != 0 {
		if req.FunctionScore, err = genFunctionScore(ctx, &httpReq.FunctionScore); err != nil {
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(merr.ErrParameterInvalid), HTTPReturnMessage: err.Error()})
			return nil, err
		}
	}
	if len(httpReq.FunctionChains) != 0 {
		if req.FunctionChains, err = genFunctionChains(httpReq.FunctionChains); err != nil {
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(merr.ErrParameterInvalid), HTTPReturnMessage: err.Error()})
			return nil, err
		}
	}

	if hasIDs {
		// Search by primary keys
		primaryField, ok := getPrimaryField(collSchema)
		if !ok {
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
				HTTPReturnMessage: "collection has no primary key field",
			})
			return nil, merr.ErrParameterInvalid
		}

		// Convert ids to schemapb.IDs
		ids, err := convertIDsToSchemapbIDs(httpReq.Ids, primaryField)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, convert ids to schemapb.IDs failed", mlog.Err(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
				HTTPReturnMessage: merr.ErrParameterInvalid.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}

		// Set Ids field using the oneof SearchInput field
		req.SearchInput = &milvuspb.SearchRequest_Ids{
			Ids: ids,
		}
		// Set anns_field in search params if provided
		if httpReq.AnnsField != "" {
			searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.AnnsFieldKey, Value: httpReq.AnnsField})
		}
	} else {
		// Search by vectors (existing logic)
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.AnnsFieldKey, Value: httpReq.AnnsField})
		body, _ := c.Get(gin.BodyBytesKey)
		placeholderGroup, err := generatePlaceholderGroup(ctx, string(body.([]byte)), collSchema, httpReq.AnnsField)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, search with vector invalid", mlog.Err(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
				HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		req.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{
			PlaceholderGroup: placeholderGroup,
		}
	}

	req.SearchParams = searchParams
	req.ExprTemplateValues = generateExpressionTemplate(httpReq.ExprParams)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Search", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Search(reqCtx, req.(*milvuspb.SearchRequest))
	})
	if err == nil {
		searchResp := resp.(*milvuspb.SearchResults)
		cost := proxy.GetCostValue(searchResp.GetStatus())
		scannedRemoteBytes, scannedTotalBytes, cacheHitRatio, isValid := proxy.GetStorageCost(searchResp.GetStatus())
		if hasSearchAggregationResult(searchResp.Results) {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildSearchAggregationResp(searchResp.Results, allowJS, collSchema)
			if err != nil {
				mlog.Warn(ctx, "high level restful api, fail to deal with search aggregation result", mlog.Any("result", searchResp.Results), mlog.Err(err))
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				respBody := gin.H{
					HTTPReturnCode:     merr.Code(nil),
					HTTPReturnData:     outputData,
					HTTPReturnCost:     cost,
					HTTPReturnAggTopks: searchResp.Results.GetAggTopks(),
				}
				if len(searchResp.Results.Recalls) > 0 {
					respBody[HTTPReturnRecalls] = searchResp.Results.Recalls
				}
				if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
					respBody[HTTPReturnScannedRemoteBytes] = scannedRemoteBytes
					respBody[HTTPReturnScannedTotalBytes] = scannedTotalBytes
					respBody[HTTPReturnCacheHitRatio] = cacheHitRatio
				}
				HTTPReturnStream(c, http.StatusOK, respBody)
			}
		} else if searchResp.Results.TopK == int64(0) {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: []interface{}{}, HTTPReturnCost: cost})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(0, searchResp.Results.OutputFields, searchResp.Results.FieldsData, searchResp.Results.Ids, searchResp.Results.Scores, allowJS, collSchema)
			if err != nil {
				mlog.Warn(ctx, "high level restful api, fail to deal with search result", mlog.Any("result", searchResp.Results), mlog.Err(err))
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				if len(searchResp.Results.Recalls) > 0 {
					if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
						HTTPReturnStream(c, http.StatusOK, gin.H{
							HTTPReturnCode:               merr.Code(nil),
							HTTPReturnData:               outputData,
							HTTPReturnCost:               cost,
							HTTPReturnRecalls:            searchResp.Results.Recalls,
							HTTPReturnTopks:              searchResp.Results.Topks,
							HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
							HTTPReturnScannedTotalBytes:  scannedTotalBytes,
							HTTPReturnCacheHitRatio:      cacheHitRatio,
						})
					} else {
						HTTPReturnStream(c, http.StatusOK, gin.H{
							HTTPReturnCode:    merr.Code(nil),
							HTTPReturnData:    outputData,
							HTTPReturnCost:    cost,
							HTTPReturnRecalls: searchResp.Results.Recalls,
							HTTPReturnTopks:   searchResp.Results.Topks,
						})
					}
				} else {
					if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
						HTTPReturnStream(c, http.StatusOK, gin.H{
							HTTPReturnCode:               merr.Code(nil),
							HTTPReturnData:               outputData,
							HTTPReturnCost:               cost,
							HTTPReturnTopks:              searchResp.Results.Topks,
							HTTPReturnScannedRemoteBytes: scannedRemoteBytes,
							HTTPReturnScannedTotalBytes:  scannedTotalBytes,
							HTTPReturnCacheHitRatio:      cacheHitRatio,
						})
					} else {
						HTTPReturnStream(c, http.StatusOK, gin.H{
							HTTPReturnCode:  merr.Code(nil),
							HTTPReturnData:  outputData,
							HTTPReturnCost:  cost,
							HTTPReturnTopks: searchResp.Results.Topks,
						})
					}
				}
			}
		}
	}
	return resp, err
}

func (h *HandlersV2) advancedSearch(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*HybridSearchReq)
	req := &milvuspb.HybridSearchRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionNames: httpReq.PartitionNames,
		Requests:       []*milvuspb.SearchRequest{},
		OutputFields:   httpReq.OutputFields,
	}
	var err error
	req.ConsistencyLevel, req.UseDefaultConsistency, err = convertConsistencyLevel(httpReq.ConsistencyLevel)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, search with consistency_level invalid", mlog.Err(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:" + err.Error(),
		})
		return nil, err
	}
	c.Set(ContextRequest, req)

	if httpReq.SearchAggregation != nil {
		err := merr.WrapErrParameterInvalidMsg("searchAggregation is not supported for hybrid search")
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}
	if len(httpReq.FunctionChains) != 0 {
		err := merr.WrapErrParameterInvalidMsg("functionChains is not supported for hybrid search yet")
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}
	for _, subReq := range httpReq.Search {
		if subReq.SearchAggregation != nil {
			err := merr.WrapErrParameterInvalidMsg("searchAggregation is not supported for hybrid search")
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			return nil, err
		}
	}

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		// has already throw http in GetCollectionSchema if fails to get schema
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	searchArray := gjson.Get(string(body.([]byte)), "search").Array()
	for i, subReq := range httpReq.Search {
		searchParams, err := generateSearchParams(subReq.SearchParams)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, generate SearchParams failed", mlog.Err(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: common.TopKKey, Value: strconv.FormatInt(int64(subReq.Limit), 10)})
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.OffsetKey, Value: strconv.FormatInt(int64(subReq.Offset), 10)})
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.AnnsFieldKey, Value: subReq.AnnsField})
		placeholderGroup, err := generatePlaceholderGroup(ctx, searchArray[i].Raw, collSchema, subReq.AnnsField)
		if err != nil {
			mlog.Warn(ctx, "high level restful api, search with vector invalid", mlog.Err(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
				HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		searchReq := &milvuspb.SearchRequest{
			DbName:         dbName,
			CollectionName: httpReq.CollectionName,
			Dsl:            subReq.Filter,
			SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{
				PlaceholderGroup: placeholderGroup,
			},
			DslType:        commonpb.DslType_BoolExprV1,
			OutputFields:   httpReq.OutputFields,
			PartitionNames: httpReq.PartitionNames,
			SearchParams:   searchParams,
		}
		searchReq.ExprTemplateValues = generateExpressionTemplate(subReq.ExprParams)
		req.Requests = append(req.Requests, searchReq)
	}

	bs, _ := json.Marshal(httpReq.Rerank.Params)
	// leave the rerank check to proxy side
	req.RankParams = []*commonpb.KeyValuePair{
		{Key: proxy.RankTypeKey, Value: httpReq.Rerank.Strategy},
		{Key: proxy.ParamsKey, Value: string(bs)},
		{Key: proxy.LimitKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)},
		{Key: proxy.OffsetKey, Value: strconv.FormatInt(int64(httpReq.Offset), 10)},
		{Key: ParamRoundDecimal, Value: "-1"},
	}
	if httpReq.GroupByField != "" {
		req.RankParams = append(req.RankParams, &commonpb.KeyValuePair{Key: ParamGroupByField, Value: httpReq.GroupByField})
	}
	if httpReq.GroupByField != "" && httpReq.GroupSize > 0 {
		req.RankParams = append(req.RankParams, &commonpb.KeyValuePair{Key: ParamGroupSize, Value: strconv.FormatInt(int64(httpReq.GroupSize), 10)})
		req.RankParams = append(req.RankParams, &commonpb.KeyValuePair{Key: ParamStrictGroupSize, Value: strconv.FormatBool(httpReq.StrictGroupSize)})
	}
	if len(httpReq.FunctionScore.Functions) != 0 {
		if req.FunctionScore, err = genFunctionScore(ctx, &httpReq.FunctionScore); err != nil {
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(merr.ErrParameterInvalid), HTTPReturnMessage: err.Error()})
			return nil, err
		}
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/HybridSearch", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.HybridSearch(reqCtx, req.(*milvuspb.HybridSearchRequest))
	})
	if err == nil {
		searchResp := resp.(*milvuspb.SearchResults)
		cost := proxy.GetCostValue(searchResp.GetStatus())
		scannedRemoteBytes, scannedTotalBytes, cacheHitRatio, isValid := proxy.GetStorageCost(searchResp.GetStatus())
		if searchResp.Results.TopK == int64(0) {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: []interface{}{}, HTTPReturnCost: cost})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(0, searchResp.Results.OutputFields, searchResp.Results.FieldsData, searchResp.Results.Ids, searchResp.Results.Scores, allowJS, collSchema)
			if err != nil {
				mlog.Warn(ctx, "high level restful api, fail to deal with search result", mlog.Any("result", searchResp.Results), mlog.Err(err))
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				if proxy.Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() && isValid {
					HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: outputData, HTTPReturnCost: cost, HTTPReturnTopks: searchResp.Results.Topks, HTTPReturnScannedRemoteBytes: scannedRemoteBytes, HTTPReturnScannedTotalBytes: scannedTotalBytes, HTTPReturnCacheHitRatio: cacheHitRatio})
				} else {
					HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: outputData, HTTPReturnCost: cost, HTTPReturnTopks: searchResp.Results.Topks})
				}
			}
		}
	}
	return resp, err
}

func defaultMetricTypeForQuickCreate(dataType schemapb.DataType) string {
	switch dataType {
	case schemapb.DataType_BinaryVector:
		return paramtable.BinaryVectorDefaultMetricType
	case schemapb.DataType_SparseFloatVector:
		return paramtable.SparseFloatVectorDefaultMetricType
	default:
		return DefaultMetricType
	}
}

func binaryMetricTypesForQuickCreate() []string {
	binaryIndexType := paramtable.Get().AutoIndexConfig.BinaryIndexParams.GetAsJSONMap()[common.IndexTypeKey]
	switch binaryIndexType {
	case "BIN_FLAT":
		return indexparamcheck.BinIDMapMetrics
	case "BIN_IVF_FLAT", "":
		return indexparamcheck.BinIvfMetrics
	default:
		return indexparamcheck.BinIvfMetrics
	}
}

func validateMetricTypeForQuickCreate(dataType schemapb.DataType, metricType string) error {
	switch dataType {
	case schemapb.DataType_BinaryVector:
		if !funcutil.SliceContain(binaryMetricTypesForQuickCreate(), metricType) {
			return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "binary vector index does not support metric type: "+metricType)
		}
	case schemapb.DataType_SparseFloatVector:
		if !funcutil.SliceContain(indexparamcheck.SparseFloatVectorMetrics, metricType) {
			return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "only IP&BM25 is the supported metric type for sparse index")
		}
		if metricType == metric.BM25 {
			return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "only BM25 Function output field support BM25 metric type")
		}
	default:
		if !funcutil.SliceContain(indexparamcheck.FloatVectorMetrics, metricType) {
			return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "float vector index does not support metric type: "+metricType)
		}
	}
	return nil
}

func consistencyLevelForCreateCollection(httpReq *CollectionReq) (commonpb.ConsistencyLevel, error) {
	consistencyLevel := commonpb.ConsistencyLevel_Bounded
	paramConsistencyLevel, hasParamConsistencyLevel := httpReq.Params["consistencyLevel"]
	paramConsistencyLevelStr := ""
	if hasParamConsistencyLevel {
		paramConsistencyLevelStr = fmt.Sprintf("%s", paramConsistencyLevel)
	}

	if httpReq.ConsistencyLevel != "" && hasParamConsistencyLevel && httpReq.ConsistencyLevel != paramConsistencyLevelStr {
		return consistencyLevel, merr.WrapErrParameterInvalid("same consistencyLevel", paramConsistencyLevel,
			"top-level consistencyLevel conflicts with params.consistencyLevel")
	}

	consistencyLevelStr := httpReq.ConsistencyLevel
	var actualConsistencyLevel interface{} = httpReq.ConsistencyLevel
	if consistencyLevelStr == "" && hasParamConsistencyLevel {
		consistencyLevelStr = paramConsistencyLevelStr
		actualConsistencyLevel = paramConsistencyLevel
	}

	if consistencyLevelStr == "" {
		return consistencyLevel, nil
	}

	if level, ok := commonpb.ConsistencyLevel_value[consistencyLevelStr]; ok {
		return commonpb.ConsistencyLevel(level), nil
	}

	return consistencyLevel, merr.WrapErrParameterInvalid("Strong, Session, Bounded, Eventually, Customized", actualConsistencyLevel,
		"consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded")
}

func (h *HandlersV2) createCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionReq)
	req := &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Properties:     []*commonpb.KeyValuePair{},
	}
	c.Set(ContextRequest, req)

	if httpReq.HasTopLevelExternalConfig() {
		err := merr.WrapErrParameterInvalid("schema.externalSource/schema.externalSpec", "top-level externalSource/externalSpec",
			"externalSource and externalSpec must be set under schema when creating an external collection")
		mlog.Warn(ctx, "high level restful api, create external collection with top-level external config fail", mlog.Err(err), mlog.Any("request", anyReq))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}

	var schema []byte
	var err error
	fieldNames := map[string]bool{}
	partitionsNum := int64(-1)
	quickCreateVectorDataType := schemapb.DataType_FloatVector
	if len(httpReq.Schema.Fields) == 0 {
		if httpReq.GetExternalSource() != "" || httpReq.GetExternalSpec() != "" {
			err := merr.WrapErrParameterInvalid("schema.fields", "empty schema fields",
				"external collection is not supported by quick create; provide schema.fields with externalField")
			mlog.Warn(ctx, "high level restful api, quickly create external collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		if len(httpReq.Schema.Functions) > 0 {
			err := merr.WrapErrParameterInvalid("schema", "functions",
				"functions are not supported for quickly create collection")
			mlog.Warn(ctx, "high level restful api, quickly create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}

		switch httpReq.VectorFieldType {
		case "", "FloatVector":
			httpReq.VectorFieldType = "FloatVector"
		case "BinaryVector":
			quickCreateVectorDataType = schemapb.DataType_BinaryVector
		case "Float16Vector":
			quickCreateVectorDataType = schemapb.DataType_Float16Vector
		case "BFloat16Vector":
			quickCreateVectorDataType = schemapb.DataType_BFloat16Vector
		case "SparseFloatVector":
			quickCreateVectorDataType = schemapb.DataType_SparseFloatVector
		default:
			err := merr.WrapErrParameterInvalid("FloatVector, BinaryVector, Float16Vector, BFloat16Vector, SparseFloatVector", httpReq.VectorFieldType,
				"vectorFieldType can only be [FloatVector, BinaryVector, Float16Vector, BFloat16Vector, SparseFloatVector], default: FloatVector")
			mlog.Warn(ctx, "high level restful api, quickly create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}

		if quickCreateVectorDataType == schemapb.DataType_SparseFloatVector && httpReq.Dimension != 0 {
			err := merr.WrapErrParameterInvalid(int32(0), httpReq.Dimension,
				"dimension should not be specified for SparseFloatVector quick create")
			mlog.Warn(ctx, "high level restful api, quickly create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}

		if quickCreateVectorDataType != schemapb.DataType_SparseFloatVector && httpReq.Dimension == 0 {
			err := merr.WrapErrParameterInvalid("collectionName & dimension", "collectionName",
				"dimension is required for quickly create collection(default metric type: "+DefaultMetricType+")")
			mlog.Warn(ctx, "high level restful api, quickly create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		if len(httpReq.MetricType) == 0 {
			httpReq.MetricType = defaultMetricTypeForQuickCreate(quickCreateVectorDataType)
		}
		if err := validateMetricTypeForQuickCreate(quickCreateVectorDataType, httpReq.MetricType); err != nil {
			mlog.Warn(ctx, "high level restful api, quickly create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		idDataType := schemapb.DataType_Int64
		idParams := []*commonpb.KeyValuePair{}
		switch httpReq.IDType {
		case "VarChar", "Varchar":
			idDataType = schemapb.DataType_VarChar
			idParams = append(idParams, &commonpb.KeyValuePair{
				Key:   common.MaxLengthKey,
				Value: fmt.Sprintf("%v", httpReq.Params["max_length"]),
			})
			httpReq.IDType = "VarChar"
		case "", "Int64", "int64":
			httpReq.IDType = "Int64"
		default:
			err := merr.WrapErrParameterInvalid("Int64, Varchar", httpReq.IDType,
				"idType can only be [Int64, VarChar], default: Int64")
			mlog.Warn(ctx, "high level restful api, quickly create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		if len(httpReq.PrimaryFieldName) == 0 {
			httpReq.PrimaryFieldName = DefaultPrimaryFieldName
		}
		if len(httpReq.VectorFieldName) == 0 {
			httpReq.VectorFieldName = DefaultVectorFieldName
		}
		enableDynamic := EnableDynamic
		if enStr, ok := httpReq.Params["enableDynamicField"]; ok {
			enableDynamic, err = strconv.ParseBool(fmt.Sprintf("%v", enStr))
			if err != nil {
				mlog.Warn(ctx, "high level restful api, parse enableDynamicField fail", mlog.Err(err), mlog.Any("request", anyReq))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: "parse enableDynamicField fail, err:" + err.Error(),
				})
				return nil, err
			}
		}
		vectorTypeParams := []*commonpb.KeyValuePair{}
		if quickCreateVectorDataType != schemapb.DataType_SparseFloatVector {
			vectorTypeParams = append(vectorTypeParams, &commonpb.KeyValuePair{
				Key:   Dim,
				Value: strconv.FormatInt(int64(httpReq.Dimension), 10),
			})
		}
		schema, err = proto.Marshal(&schemapb.CollectionSchema{
			Name: httpReq.CollectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         httpReq.PrimaryFieldName,
					IsPrimaryKey: true,
					DataType:     idDataType,
					AutoID:       httpReq.AutoID,
					TypeParams:   idParams,
				},
				{
					FieldID:      common.StartOfUserFieldID + 1,
					Name:         httpReq.VectorFieldName,
					IsPrimaryKey: false,
					DataType:     quickCreateVectorDataType,
					TypeParams:   vectorTypeParams,
					AutoID:       DisableAutoID,
				},
			},
			EnableDynamicField: enableDynamic,
			Description:        httpReq.Description,
			ExternalSource:     httpReq.GetExternalSource(),
			ExternalSpec:       httpReq.GetExternalSpec(),
		})
	} else {
		collSchema := schemapb.CollectionSchema{
			Name:               httpReq.CollectionName,
			AutoID:             httpReq.Schema.AutoId,
			Fields:             []*schemapb.FieldSchema{},
			StructArrayFields:  []*schemapb.StructArrayFieldSchema{},
			Functions:          []*schemapb.FunctionSchema{},
			EnableDynamicField: httpReq.Schema.EnableDynamicField,
			Description:        httpReq.Description,
			ExternalSource:     httpReq.GetExternalSource(),
			ExternalSpec:       httpReq.GetExternalSpec(),
		}

		allOutputFields := []string{}

		for _, function := range httpReq.Schema.Functions {
			f, err := genFunctionSchema(ctx, &function)
			if err != nil {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
					HTTPReturnMessage: err.Error(),
				})
				return nil, err
			}

			collSchema.Functions = append(collSchema.Functions, f)
			allOutputFields = append(allOutputFields, function.OutputFieldNames...)
		}

		for _, field := range httpReq.Schema.Fields {
			fieldDataType, ok := schemapb.DataType_value[field.DataType]
			if !ok {
				mlog.Warn(ctx, "field's data type is invalid(case sensitive).", mlog.Any("fieldDataType", field.DataType), mlog.Any("field", field))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
					HTTPReturnMessage: merr.ErrParameterInvalid.Error() + ", data type " + field.DataType + " is invalid(case sensitive).",
				})
				return nil, merr.ErrParameterInvalid
			}
			dataType := schemapb.DataType(fieldDataType)
			fieldSchema := schemapb.FieldSchema{
				Name:            field.FieldName,
				IsPrimaryKey:    field.IsPrimary,
				IsPartitionKey:  field.IsPartitionKey,
				IsClusteringKey: field.IsClusteringKey,
				DataType:        dataType,
				TypeParams:      []*commonpb.KeyValuePair{},
				Nullable:        field.Nullable,
				ExternalField:   field.ExternalField,
			}

			fieldSchema.DefaultValue, err = convertDefaultValue(field.DefaultValue, dataType)
			if err != nil {
				mlog.Warn(ctx, "convert defaultValue fail", mlog.Any("defaultValue", field.DefaultValue))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: "convert defaultValue fail, err:" + err.Error(),
				})
				return nil, err
			}
			if dataType == schemapb.DataType_Array {
				if _, ok := schemapb.DataType_value[field.ElementDataType]; !ok {
					mlog.Warn(ctx, "element's data type is invalid(case sensitive).", mlog.Any("elementDataType", field.ElementDataType), mlog.Any("field", field))
					HTTPAbortReturn(c, http.StatusOK, gin.H{
						HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
						HTTPReturnMessage: merr.ErrParameterInvalid.Error() + ", element data type " + field.ElementDataType + " is invalid(case sensitive).",
					})
					return nil, merr.ErrParameterInvalid
				}
				fieldSchema.ElementType = schemapb.DataType(schemapb.DataType_value[field.ElementDataType])
			}
			if field.IsPrimary {
				fieldSchema.AutoID = httpReq.Schema.AutoId
			}
			if field.IsPartitionKey {
				if partitionsNumStr, ok := httpReq.Params["partitionsNum"]; ok {
					if partitions, err := strconv.ParseInt(fmt.Sprintf("%v", partitionsNumStr), 10, 64); err == nil {
						partitionsNum = partitions
					}
				}
			}
			for key, fieldParam := range field.ElementTypeParams {
				value, err := getElementTypeParams(fieldParam)
				if err != nil {
					return nil, err
				}
				fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{Key: key, Value: value})
			}
			if lo.Contains(allOutputFields, field.FieldName) {
				fieldSchema.IsFunctionOutput = true
			}
			collSchema.Fields = append(collSchema.Fields, &fieldSchema)
			fieldNames[field.FieldName] = true
		}
		for i := range httpReq.Schema.StructFields {
			structField := httpReq.Schema.StructFields[i]
			if _, dup := fieldNames[structField.FieldName]; dup {
				err := merr.WrapErrParameterInvalidMsg("duplicated field name: %s", structField.FieldName)
				mlog.Warn(ctx, "high level restful api, create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: err.Error(),
				})
				return nil, err
			}
			structProto, err := structField.GetProto(ctx)
			if err != nil {
				mlog.Warn(ctx, "high level restful api, convert struct array field fail",
					mlog.String("structField", structField.FieldName), mlog.Err(err))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: err.Error(),
				})
				return nil, err
			}
			collSchema.StructArrayFields = append(collSchema.StructArrayFields, structProto)
			fieldNames[structField.FieldName] = true
			for _, sub := range structProto.GetFields() {
				qualified := typeutil.ConcatStructFieldName(structField.FieldName, sub.GetName())
				fieldNames[qualified] = true
			}
		}
		schema, err = proto.Marshal(&collSchema)
	}
	if err != nil {
		mlog.Warn(ctx, "high level restful api, marshal collection schema fail", mlog.Err(err), mlog.Any("request", anyReq))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMarshalCollectionSchema),
			HTTPReturnMessage: merr.ErrMarshalCollectionSchema.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req.Schema = schema

	shardsNum := int32(ShardNumDefault)
	if shardsNumStr, ok := httpReq.Params["shardsNum"]; ok {
		if shards, err := strconv.ParseInt(fmt.Sprintf("%v", shardsNumStr), 10, 64); err == nil {
			shardsNum = int32(shards)
		}
	}
	req.ShardsNum = shardsNum

	consistencyLevel, err := consistencyLevelForCreateCollection(httpReq)
	if err != nil {
		mlog.Warn(ctx, "high level restful api, create collection fail", mlog.Err(err), mlog.Any("request", anyReq))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: err.Error(),
		})
		return nil, err
	}
	req.ConsistencyLevel = consistencyLevel

	if partitionsNum > 0 {
		req.NumPartitions = partitionsNum
	}
	ttlPropertySet := false
	for _, propertyKey := range []string{common.CollectionTTLConfigKey, "ttl.seconds"} {
		value, ok := httpReq.Properties[propertyKey]
		if !ok {
			continue
		}
		if ttlPropertySet {
			err := merr.WrapErrParameterInvalidMsg("collection TTL is specified multiple times")
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		ttlPropertySet = true
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: fmt.Sprintf("%v", value)})
	}
	if _, ok := httpReq.Params["ttlSeconds"]; ok {
		if ttlPropertySet {
			err := merr.WrapErrParameterInvalidMsg("collection TTL is specified multiple times")
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLConfigKey,
			Value: fmt.Sprintf("%v", httpReq.Params["ttlSeconds"]),
		})
	}
	if _, ok := httpReq.Params["ttlField"]; ok {
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLFieldKey,
			Value: fmt.Sprintf("%v", httpReq.Params["ttlField"]),
		})
	}
	if _, ok := httpReq.Params["partitionKeyIsolation"]; ok {
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{
			Key:   common.PartitionKeyIsolationKey,
			Value: fmt.Sprintf("%v", httpReq.Params["partitionKeyIsolation"]),
		})
	}
	if _, ok := httpReq.Params[common.MmapEnabledKey]; ok {
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{
			Key:   common.MmapEnabledKey,
			Value: fmt.Sprintf("%v", httpReq.Params[common.MmapEnabledKey]),
		})
	}
	for _, key := range []string{
		common.WarmupScalarFieldKey,
		common.WarmupScalarIndexKey,
		common.WarmupVectorFieldKey,
		common.WarmupVectorIndexKey,
	} {
		if _, ok := httpReq.Params[key]; ok {
			req.Properties = append(req.Properties, &commonpb.KeyValuePair{
				Key:   key,
				Value: fmt.Sprintf("%v", httpReq.Params[key]),
			})
		}
	}

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateCollection(reqCtx, req.(*milvuspb.CreateCollectionRequest))
	})
	if err != nil {
		return resp, err
	}
	if len(httpReq.Schema.Fields) == 0 {
		createIndexReq := &milvuspb.CreateIndexRequest{
			DbName:         dbName,
			CollectionName: httpReq.CollectionName,
			FieldName:      httpReq.VectorFieldName,
			IndexName:      httpReq.VectorFieldName,
			ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: httpReq.MetricType}},
		}
		statusResponse, err := wrapperProxyWithLimit(ctx, c, createIndexReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateIndex", false, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CreateIndex(ctx, req.(*milvuspb.CreateIndexRequest))
		})
		if err != nil {
			return statusResponse, err
		}
	} else {
		if len(httpReq.IndexParams) == 0 {
			HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
			return nil, nil
		}
		for _, indexParam := range httpReq.IndexParams {
			if _, ok := fieldNames[indexParam.FieldName]; !ok {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
					HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", error: `" + indexParam.FieldName + "` hasn't defined in schema",
				})
				return nil, merr.ErrMissingRequiredParameters
			}
			createIndexReq := &milvuspb.CreateIndexRequest{
				DbName:         dbName,
				CollectionName: httpReq.CollectionName,
				FieldName:      indexParam.FieldName,
				IndexName:      indexParam.IndexName,
				ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: indexParam.MetricType}},
			}
			createIndexReq.ExtraParams, err = convertToExtraParams(indexParam)
			if err != nil {
				// will not happen
				mlog.Warn(ctx, "high level restful api, convertToExtraParams fail", mlog.Err(err), mlog.Any("request", anyReq))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: err.Error(),
				})
				return resp, err
			}
			statusResponse, err := wrapperProxyWithLimit(ctx, c, createIndexReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateIndex", false, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
				return h.proxy.CreateIndex(ctx, req.(*milvuspb.CreateIndexRequest))
			})
			if err != nil {
				return statusResponse, err
			}
		}
	}
	loadReq := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	statusResponse, err := wrapperProxyWithLimit(ctx, c, loadReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/LoadCollection", false, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadCollection(ctx, req.(*milvuspb.LoadCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return statusResponse, err
}

func (h *HandlersV2) createDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DatabaseReqWithProperties)
	req := &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateDatabase", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateDatabase(reqCtx, req.(*milvuspb.CreateDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.DropDatabaseRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropDatabase", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropDatabase(reqCtx, req.(*milvuspb.DropDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropDatabaseProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DropDatabasePropertiesReq)
	req := &milvuspb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: httpReq.PropertyKeys,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterDatabase", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterDatabase(reqCtx, req.(*milvuspb.AlterDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// todo: use a more flexible way to handle the number of input parameters of req
func (h *HandlersV2) listDatabases(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListDatabasesRequest{}
	c.Set(ContextRequest, req)
	// handle at rootcoord side
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ListDatabases", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListDatabases(reqCtx, req.(*milvuspb.ListDatabasesRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListDatabasesResponse).DbNames))
	}
	return resp, err
}

func (h *HandlersV2) describeDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.DescribeDatabaseRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeDatabase", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeDatabase(reqCtx, req.(*milvuspb.DescribeDatabaseRequest))
	})
	if err != nil {
		return nil, err
	}
	info, _ := resp.(*milvuspb.DescribeDatabaseResponse)
	if info.Properties == nil {
		info.Properties = []*commonpb.KeyValuePair{}
	}
	dataBaseInfo := map[string]any{
		HTTPDbName:     info.DbName,
		HTTPDbID:       info.DbID,
		HTTPProperties: info.Properties,
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: dataBaseInfo})
	return resp, err
}

func (h *HandlersV2) alterDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DatabaseReqWithProperties)
	req := &milvuspb.AlterDatabaseRequest{
		DbName: dbName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterDatabase", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterDatabase(reqCtx, req.(*milvuspb.AlterDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ShowPartitionsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ShowPartitions", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ShowPartitions(reqCtx, req.(*milvuspb.ShowPartitionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ShowPartitionsResponse).PartitionNames))
	}
	return resp, err
}

func (h *HandlersV2) hasPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.HasPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/HasPartition", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.HasPartition(reqCtx, req.(*milvuspb.HasPartitionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnHas(resp.(*milvuspb.BoolResponse).Value))
	}
	return resp, err
}

// data coord will collect partitions' row_count
// proxy grpc call only support partition not partitions
func (h *HandlersV2) statsPartition(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.GetPartitionStatisticsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetPartitionStatistics", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetPartitionStatistics(reqCtx, req.(*milvuspb.GetPartitionStatisticsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnRowCount(resp.(*milvuspb.GetPartitionStatisticsResponse).Stats))
	}
	return resp, err
}

func (h *HandlersV2) createPartition(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreatePartition", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreatePartition(reqCtx, req.(*milvuspb.CreatePartitionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropPartition(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropPartition", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropPartition(reqCtx, req.(*milvuspb.DropPartitionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) loadPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PartitionsReq)
	req := &milvuspb.LoadPartitionsRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionNames: httpReq.PartitionNames,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/LoadPartitions", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadPartitions(reqCtx, req.(*milvuspb.LoadPartitionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) releasePartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PartitionsReq)
	req := &milvuspb.ReleasePartitionsRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionNames: httpReq.PartitionNames,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ReleasePartitions", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ReleasePartitions(reqCtx, req.(*milvuspb.ReleasePartitionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listUsers(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListCredUsersRequest{}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListCredUsers", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListCredUsers(reqCtx, req.(*milvuspb.ListCredUsersRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListCredUsersResponse).Usernames))
	}
	return resp, err
}

func (h *HandlersV2) describeUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	userNameGetter, _ := anyReq.(UserNameGetter)
	userName := userNameGetter.GetUserName()
	req := &milvuspb.SelectUserRequest{
		User: &milvuspb.UserEntity{
			Name: userName,
		},
		IncludeRoleInfo: true,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectUser", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectUser(reqCtx, req.(*milvuspb.SelectUserRequest))
	})
	if err == nil {
		roleNames := []string{}
		description := ""
		for _, userRole := range resp.(*milvuspb.SelectUserResponse).Results {
			if userRole.User.Name == userName {
				description = userRole.GetDescription()
				for _, role := range userRole.Roles {
					roleNames = append(roleNames, role.Name)
				}
			}
		}
		result := wrapperReturnList(roleNames)
		result[HTTPReturnDescription] = description
		HTTPReturn(c, http.StatusOK, result)
	}
	return resp, err
}

func (h *HandlersV2) createUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PasswordReq)
	req := &milvuspb.CreateCredentialRequest{
		Username:    httpReq.UserName,
		Password:    crypto.Base64Encode(httpReq.Password),
		Description: httpReq.Description,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateCredential", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateCredential(reqCtx, req.(*milvuspb.CreateCredentialRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) updateUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*NewPasswordReq)
	req := &milvuspb.UpdateCredentialRequest{
		Username:    httpReq.UserName,
		Description: httpReq.Description,
	}
	if httpReq.NewPassword != "" {
		req.OldPassword = crypto.Base64Encode(httpReq.Password)
		req.NewPassword = crypto.Base64Encode(httpReq.NewPassword)
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/UpdateCredential", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.UpdateCredential(reqCtx, req.(*milvuspb.UpdateCredentialRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(UserNameGetter)
	req := &milvuspb.DeleteCredentialRequest{
		Username: getter.GetUserName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DeleteCredential", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DeleteCredential(reqCtx, req.(*milvuspb.DeleteCredentialRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operateRoleToUser(ctx context.Context, c *gin.Context, userName, roleName string, operateType milvuspb.OperateUserRoleType) (interface{}, error) {
	req := &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
		Type:     operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperateUserRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperateUserRole(reqCtx, req.(*milvuspb.OperateUserRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) addRoleToUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operateRoleToUser(ctx, c, anyReq.(*UserRoleReq).UserName, anyReq.(*UserRoleReq).RoleName, milvuspb.OperateUserRoleType_AddUserToRole)
}

func (h *HandlersV2) removeRoleFromUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operateRoleToUser(ctx, c, anyReq.(*UserRoleReq).UserName, anyReq.(*UserRoleReq).RoleName, milvuspb.OperateUserRoleType_RemoveUserFromRole)
}

func (h *HandlersV2) listRoles(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.SelectRoleRequest{}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectRole(reqCtx, req.(*milvuspb.SelectRoleRequest))
	})
	if err == nil {
		roleNames := []string{}
		for _, role := range resp.(*milvuspb.SelectRoleResponse).Results {
			roleNames = append(roleNames, role.Role.Name)
		}
		HTTPReturn(c, http.StatusOK, wrapperReturnList(roleNames))
	}
	return resp, err
}

func (h *HandlersV2) describeRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	roleReq := &milvuspb.SelectRoleRequest{
		Role: &milvuspb.RoleEntity{Name: getter.GetRoleName()},
	}
	roleResp, err := wrapperProxy(ctx, c, roleReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectRole(reqCtx, req.(*milvuspb.SelectRoleRequest))
	})
	if err != nil {
		return roleResp, err
	}
	description := ""
	if results := roleResp.(*milvuspb.SelectRoleResponse).GetResults(); len(results) > 0 {
		description = results[0].GetRole().GetDescription()
	}
	req := &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: getter.GetRoleName()}, DbName: dbName},
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectGrant", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectGrant(reqCtx, req.(*milvuspb.SelectGrantRequest))
	})
	if err == nil {
		privileges := [](map[string]string){}
		for _, grant := range resp.(*milvuspb.SelectGrantResponse).Entities {
			privilege := map[string]string{
				HTTPReturnObjectType: grant.Object.Name,
				HTTPReturnObjectName: grant.ObjectName,
				HTTPReturnPrivilege:  grant.Grantor.Privilege.Name,
				HTTPReturnDbName:     grant.DbName,
				HTTPReturnGrantor:    grant.Grantor.User.Name,
			}
			privileges = append(privileges, privilege)
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: privileges, HTTPReturnDescription: description})
	}
	return resp, err
}

func (h *HandlersV2) createRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RoleReq)
	req := &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: httpReq.GetRoleName(), Description: httpReq.GetDescription()},
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateRole(reqCtx, req.(*milvuspb.CreateRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RoleReq)
	req := &milvuspb.AlterRoleRequest{
		RoleName:    httpReq.GetRoleName(),
		Description: httpReq.GetDescription(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterRole(reqCtx, req.(*milvuspb.AlterRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.DropRoleRequest{
		RoleName: getter.GetRoleName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropRole(reqCtx, req.(*milvuspb.DropRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operatePrivilegeToRole(ctx context.Context, c *gin.Context, httpReq *GrantReq, operateType milvuspb.OperatePrivilegeType, dbName string) (interface{}, error) {
	req := &milvuspb.OperatePrivilegeRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: httpReq.RoleName},
			Object:     &milvuspb.ObjectEntity{Name: httpReq.ObjectType},
			ObjectName: httpReq.ObjectName,
			DbName:     dbName,
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: httpReq.Privilege},
			},
		},
		Type: operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperatePrivilege", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilege(reqCtx, req.(*milvuspb.OperatePrivilegeRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operatePrivilegeToRoleV2(ctx context.Context, c *gin.Context, httpReq *GrantV2Req, operateType milvuspb.OperatePrivilegeType) (interface{}, error) {
	req := &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: httpReq.RoleName},
		Grantor: &milvuspb.GrantorEntity{
			Privilege: &milvuspb.PrivilegeEntity{Name: httpReq.Privilege},
		},
		Type:           operateType,
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperatePrivilegeV2", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilegeV2(reqCtx, req.(*milvuspb.OperatePrivilegeV2Request))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) grantV2(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRoleV2(ctx, c, anyReq.(*GrantV2Req), milvuspb.OperatePrivilegeType_Grant)
}

func (h *HandlersV2) revokeV2(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRoleV2(ctx, c, anyReq.(*GrantV2Req), milvuspb.OperatePrivilegeType_Revoke)
}

func (h *HandlersV2) addPrivilegeToRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRole(ctx, c, anyReq.(*GrantReq), milvuspb.OperatePrivilegeType_Grant, dbName)
}

func (h *HandlersV2) removePrivilegeFromRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRole(ctx, c, anyReq.(*GrantReq), milvuspb.OperatePrivilegeType_Revoke, dbName)
}

func (h *HandlersV2) createPrivilegeGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PrivilegeGroupReq)
	req := &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: httpReq.PrivilegeGroupName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreatePrivilegeGroup", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreatePrivilegeGroup(reqCtx, req.(*milvuspb.CreatePrivilegeGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropPrivilegeGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PrivilegeGroupReq)
	req := &milvuspb.DropPrivilegeGroupRequest{
		GroupName: httpReq.PrivilegeGroupName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropPrivilegeGroup", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropPrivilegeGroup(reqCtx, req.(*milvuspb.DropPrivilegeGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listPrivilegeGroups(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListPrivilegeGroupsRequest{}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListPrivilegeGroups", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListPrivilegeGroups(reqCtx, req.(*milvuspb.ListPrivilegeGroupsRequest))
	})
	if err == nil {
		privGroups := make([]map[string]interface{}, 0)
		for _, group := range resp.(*milvuspb.ListPrivilegeGroupsResponse).PrivilegeGroups {
			privileges := make([]string, len(group.Privileges))
			for i, privilege := range group.Privileges {
				privileges[i] = privilege.Name
			}
			groupInfo := map[string]interface{}{
				HTTPReturnPrivilegeGroupName: group.GroupName,
				HTTPReturnPrivileges:         strings.Join(privileges, ","),
			}
			privGroups = append(privGroups, groupInfo)
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
			HTTPReturnPrivilegeGroups: privGroups,
		}})
	}
	return resp, err
}

func (h *HandlersV2) addPrivilegesToGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeGroup(ctx, c, anyReq, dbName, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup)
}

func (h *HandlersV2) removePrivilegesFromGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeGroup(ctx, c, anyReq, dbName, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
}

func (h *HandlersV2) operatePrivilegeGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string, operateType milvuspb.OperatePrivilegeGroupType) (interface{}, error) {
	httpReq := anyReq.(*PrivilegeGroupReq)
	req := &milvuspb.OperatePrivilegeGroupRequest{
		GroupName: httpReq.PrivilegeGroupName,
		Privileges: lo.Map(httpReq.Privileges, func(p string, _ int) *milvuspb.PrivilegeEntity {
			return &milvuspb.PrivilegeEntity{Name: p}
		}),
		Type: operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperatePrivilegeGroup", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilegeGroup(reqCtx, req.(*milvuspb.OperatePrivilegeGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listIndexes(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexNames := []string{}
	req := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeIndex", func(reqCtx context.Context, req any) (any, error) {
		resp, err := h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
		if errors.Is(err, merr.ErrIndexNotFound) {
			return &milvuspb.DescribeIndexResponse{
				IndexDescriptions: []*milvuspb.IndexDescription{},
			}, nil
		}
		if resp != nil && errors.Is(merr.Error(resp.Status), merr.ErrIndexNotFound) {
			return &milvuspb.DescribeIndexResponse{
				IndexDescriptions: []*milvuspb.IndexDescription{},
			}, nil
		}
		return resp, err
	})
	if err != nil {
		return resp, err
	}
	for _, index := range resp.(*milvuspb.DescribeIndexResponse).IndexDescriptions {
		indexNames = append(indexNames, index.IndexName)
	}
	HTTPReturn(c, http.StatusOK, wrapperReturnList(indexNames))
	return resp, err
}

func (h *HandlersV2) describeIndex(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexGetter, _ := anyReq.(IndexNameGetter)
	timeGetter, _ := anyReq.(TimestampGetter)

	req := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		IndexName:      indexGetter.GetIndexName(),
		Timestamp:      timeGetter.GetTimestamp(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeIndex", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err == nil {
		indexInfos := [](map[string]any){}
		for _, indexDescription := range resp.(*milvuspb.DescribeIndexResponse).IndexDescriptions {
			metricType := ""
			indexType := ""
			mmapEnabled := ""
			indexOffsetCacheEnabled := ""
			warmup := ""
			for _, pair := range indexDescription.Params {
				switch pair.Key {
				case common.MetricTypeKey:
					metricType = pair.Value
				case common.IndexTypeKey:
					indexType = pair.Value
				case common.MmapEnabledKey:
					mmapEnabled = pair.Value
				case common.IndexOffsetCacheEnabledKey:
					indexOffsetCacheEnabled = pair.Value
				case common.WarmupKey:
					warmup = pair.Value
				}
			}
			indexInfo := map[string]any{
				HTTPIndexName:                  indexDescription.IndexName,
				HTTPIndexField:                 indexDescription.FieldName,
				HTTPReturnIndexType:            indexType,
				HTTPReturnIndexMetricType:      metricType,
				HTTPMmapEnabledKey:             mmapEnabled,
				HTTPIndexOffsetCacheEnabledKey: indexOffsetCacheEnabled,
				HTTPWarmupKey:                  warmup,
				HTTPReturnIndexTotalRows:       indexDescription.TotalRows,
				HTTPReturnIndexPendingRows:     indexDescription.PendingIndexRows,
				HTTPReturnIndexIndexedRows:     indexDescription.IndexedRows,
				HTTPReturnIndexState:           indexDescription.State.String(),
				HTTPReturnIndexFailReason:      indexDescription.IndexStateFailReason,
				HTTPReturnMinIndexVersion:      indexDescription.MinIndexVersion,
				HTTPReturnMaxIndexVersion:      indexDescription.MaxIndexVersion,
				HTTPReturnIndexParams:          indexDescription.Params,
			}
			indexInfos = append(indexInfos, indexInfo)
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: indexInfos})
	}
	return resp, err
}

func (h *HandlersV2) createIndex(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*IndexParamReq)
	for _, indexParam := range httpReq.IndexParams {
		req := &milvuspb.CreateIndexRequest{
			DbName:         dbName,
			CollectionName: httpReq.CollectionName,
			FieldName:      indexParam.FieldName,
			IndexName:      indexParam.IndexName,
			ExtraParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: indexParam.MetricType},
			},
		}
		c.Set(ContextRequest, req)

		var err error
		req.ExtraParams, err = convertToExtraParams(indexParam)
		if err != nil {
			// will not happen
			mlog.Warn(ctx, "high level restful api, convertToExtraParams fail", mlog.Err(err), mlog.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CreateIndex(reqCtx, req.(*milvuspb.CreateIndexRequest))
		})
		if err != nil {
			return resp, err
		}
	}
	HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	return httpReq.IndexParams, nil
}

func (h *HandlersV2) dropIndex(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexGetter, _ := anyReq.(IndexNameGetter)
	req := &milvuspb.DropIndexRequest{
		DbName:         dbName,
		CollectionName: collGetter.GetCollectionName(),
		IndexName:      indexGetter.GetIndexName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropIndex(reqCtx, req.(*milvuspb.DropIndexRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterIndexProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*IndexReqWithProperties)
	req := &milvuspb.AlterIndexRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		IndexName:      httpReq.IndexName,
	}
	extraParams := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		extraParams = append(extraParams, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.ExtraParams = extraParams

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterIndex(reqCtx, req.(*milvuspb.AlterIndexRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropIndexProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DropIndexPropertiesReq)
	req := &milvuspb.AlterIndexRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		DeleteKeys:     httpReq.PropertyKeys,
		IndexName:      httpReq.IndexName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterIndex(reqCtx, req.(*milvuspb.AlterIndexRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ListAliasesRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListAliases", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListAliases(reqCtx, req.(*milvuspb.ListAliasesRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListAliasesResponse).Aliases))
	}
	return resp, err
}

func (h *HandlersV2) describeAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.DescribeAliasRequest{
		DbName: dbName,
		Alias:  getter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeAlias", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeAlias(reqCtx, req.(*milvuspb.DescribeAliasRequest))
	})
	if err == nil {
		response := resp.(*milvuspb.DescribeAliasResponse)
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
			HTTPDbName:         response.DbName,
			HTTPCollectionName: response.Collection,
			HTTPAliasName:      response.Alias,
		}})
	}
	return resp, err
}

func (h *HandlersV2) createAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	aliasGetter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.CreateAliasRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		Alias:          aliasGetter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateAlias", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateAlias(reqCtx, req.(*milvuspb.CreateAliasRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.DropAliasRequest{
		DbName: dbName,
		Alias:  getter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropAlias", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropAlias(reqCtx, req.(*milvuspb.DropAliasRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	aliasGetter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.AlterAliasRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		Alias:          aliasGetter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterAlias", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterAlias(reqCtx, req.(*milvuspb.AlterAliasRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	var collectionName string
	if collectionGetter, ok := anyReq.(requestutil.CollectionNameGetter); ok {
		collectionName = collectionGetter.GetCollectionName()
	}
	req := &internalpb.ListImportsRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ListImports", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListImports(reqCtx, req.(*internalpb.ListImportsRequest))
	})
	if err == nil {
		returnData := make(map[string]interface{})
		records := make([]map[string]interface{}, 0)
		response := resp.(*internalpb.ListImportsResponse)
		jobIDs := response.GetJobIDs()
		states := response.GetStates()
		progresses := response.GetProgresses()
		reasons := response.GetReasons()
		collections := response.GetCollectionNames()

		for i, jobID := range jobIDs {
			collection := collections[i]
			if h.checkAuth {
				err = checkAuthorizationHelper(ctx, c, &milvuspb.ImportAuthPlaceholder{
					DbName:         dbName,
					CollectionName: collection,
				})
				if err != nil {
					continue
				}
			}
			jobDetail := make(map[string]interface{})
			jobDetail["jobId"] = jobID
			jobDetail["collectionName"] = collection
			jobDetail["state"] = states[i].String()
			jobDetail["progress"] = progresses[i]
			reason := reasons[i]
			if reason != "" {
				jobDetail["reason"] = reason
			}
			records = append(records, jobDetail)
		}
		returnData["records"] = records
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) createImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	var (
		collectionGetter = anyReq.(requestutil.CollectionNameGetter)
		partitionGetter  = anyReq.(requestutil.PartitionNameGetter)
		filesGetter      = anyReq.(FilesGetter)
		optionsGetter    = anyReq.(OptionsGetter)
	)
	req := &internalpb.ImportRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
		Files: lo.Map(filesGetter.GetFiles(), func(paths []string, _ int) *internalpb.ImportFile {
			return &internalpb.ImportFile{Paths: paths}
		}),
		Options: funcutil.Map2KeyValuePair(optionsGetter.GetOptions()),
	}
	c.Set(ContextRequest, req)

	if h.checkAuth {
		err := checkAuthorizationV2(ctx, c, false, &milvuspb.ImportAuthPlaceholder{
			DbName:         dbName,
			CollectionName: collectionGetter.GetCollectionName(),
			PartitionName:  partitionGetter.GetPartitionName(),
		})
		if err != nil {
			return nil, err
		}
		ctx = c.Request.Context()
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/Import", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ImportV2(reqCtx, req.(*internalpb.ImportRequest))
	})
	if err == nil {
		returnData := make(map[string]interface{})
		returnData["jobId"] = resp.(*internalpb.ImportResponse).GetJobID()
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) getImportJobProcess(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	jobIDGetter := anyReq.(JobIDGetter)
	response, err := h.getImportProgress(ctx, c, dbName, jobIDGetter.GetJobID())
	if err != nil {
		return response, err
	}
	if err := h.checkImportPrivilege(ctx, c, dbName, response.GetCollectionName()); err != nil {
		return nil, err
	}

	returnData := make(map[string]interface{})
	returnData["jobId"] = jobIDGetter.GetJobID()
	returnData["createTime"] = response.GetCreateTime()
	returnData["collectionName"] = response.GetCollectionName()
	returnData["completeTime"] = response.GetCompleteTime()
	returnData["state"] = response.GetState().String()
	returnData["progress"] = response.GetProgress()
	returnData["importedRows"] = response.GetImportedRows()
	returnData["totalRows"] = response.GetTotalRows()
	reason := response.GetReason()
	if reason != "" {
		returnData["reason"] = reason
	}
	details := make([]map[string]interface{}, 0)
	totalFileSize := int64(0)
	for _, taskProgress := range response.GetTaskProgresses() {
		detail := make(map[string]interface{})
		detail["fileName"] = taskProgress.GetFileName()
		detail["fileSize"] = taskProgress.GetFileSize()
		detail["progress"] = taskProgress.GetProgress()
		detail["completeTime"] = taskProgress.GetCompleteTime()
		detail["state"] = taskProgress.GetState()
		detail["importedRows"] = taskProgress.GetImportedRows()
		detail["totalRows"] = taskProgress.GetTotalRows()
		reason = taskProgress.GetReason()
		if reason != "" {
			detail["reason"] = reason
		}
		details = append(details, detail)
		totalFileSize += taskProgress.GetFileSize()
	}
	returnData["fileSize"] = totalFileSize
	returnData["details"] = details
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	return response, nil
}

func (h *HandlersV2) getImportProgress(ctx context.Context, c *gin.Context, dbName string, jobID string) (*internalpb.GetImportProgressResponse, error) {
	req := &internalpb.GetImportProgressRequest{
		DbName: dbName,
		JobID:  jobID,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/GetImportProgress", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetImportProgress(reqCtx, req.(*internalpb.GetImportProgressRequest))
	})
	if err != nil {
		return nil, err
	}
	return resp.(*internalpb.GetImportProgressResponse), nil
}

func (h *HandlersV2) checkImportPrivilege(ctx context.Context, c *gin.Context, dbName string, collectionName string) error {
	if !h.checkAuth {
		return nil
	}

	authErr := checkAuthorizationHelper(ctx, c, &milvuspb.ImportAuthPlaceholder{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if authErr != nil {
		HTTPReturn(c, http.StatusForbidden, gin.H{
			HTTPReturnCode:    merr.Code(authErr),
			HTTPReturnMessage: authErr.Error(),
		})
		return authErr
	}
	return nil
}

func (h *HandlersV2) checkImportJobAuth(ctx context.Context, c *gin.Context, dbName string, jobID string) error {
	if !h.checkAuth {
		return nil
	}

	// Commit/abort requests only carry jobID. Import privilege is collection-scoped,
	// so resolve the job's collection before checking PrivilegeImport.
	response, err := h.getImportProgress(ctx, c, dbName, jobID)
	if err != nil {
		return err
	}
	return h.checkImportPrivilege(ctx, c, dbName, response.GetCollectionName())
}

func (h *HandlersV2) restoreExternalSnapshot(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RestoreExternalSnapshotReq)
	req := &milvuspb.RestoreExternalSnapshotRequest{
		DbName:               dbName,
		TargetCollectionName: httpReq.TargetCollectionName,
		SnapshotMetadataUri:  httpReq.SnapshotMetadataURI,
		ExternalSpec:         httpReq.ExternalSpec,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RestoreExternalSnapshot", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RestoreExternalSnapshot(reqCtx, req.(*milvuspb.RestoreExternalSnapshotRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"jobId": resp.(*milvuspb.RestoreExternalSnapshotResponse).GetJobId()},
		})
	}
	return resp, err
}

func (h *HandlersV2) exportSnapshot(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*ExportSnapshotReq)
	req := &milvuspb.ExportSnapshotRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Name:           httpReq.Name,
		TargetS3Path:   httpReq.TargetS3Path,
		ExternalSpec:   httpReq.ExternalSpec,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ExportSnapshot", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ExportSnapshot(reqCtx, req.(*milvuspb.ExportSnapshotRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"snapshotMetadataURI": resp.(*milvuspb.ExportSnapshotResponse).GetSnapshotMetadataUri()},
		})
	}
	return resp, err
}

func restoreSnapshotJobToREST(info *milvuspb.RestoreSnapshotInfo) gin.H {
	if info == nil {
		return gin.H{}
	}
	return gin.H{
		"jobId":          info.GetJobId(),
		"snapshotName":   info.GetSnapshotName(),
		"dbName":         info.GetDbName(),
		"collectionName": info.GetCollectionName(),
		"state":          info.GetState().String(),
		"progress":       info.GetProgress(),
		"reason":         info.GetReason(),
		"startTime":      info.GetStartTime(),
		"timeCost":       info.GetTimeCost(),
	}
}

func (h *HandlersV2) getRestoreSnapshotState(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*JobIDReq)
	jobID, err := strconv.ParseInt(httpReq.GetJobID(), 10, 64)
	if err != nil {
		paramErr := merr.WrapErrParameterInvalid("int64 jobId", httpReq.GetJobID(), err.Error())
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(paramErr), HTTPReturnMessage: paramErr.Error()})
		return nil, paramErr
	}
	req := &milvuspb.GetRestoreSnapshotStateRequest{
		Base:  commonpbutil.NewMsgBase(),
		JobId: jobID,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetRestoreSnapshotState", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetRestoreSnapshotState(reqCtx, req.(*milvuspb.GetRestoreSnapshotStateRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: restoreSnapshotJobToREST(resp.(*milvuspb.GetRestoreSnapshotStateResponse).GetInfo()),
		})
	}
	return resp, err
}

func (h *HandlersV2) listRestoreSnapshotJobs(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*OptionalCollectionNameReq)
	req := &milvuspb.ListRestoreSnapshotJobsRequest{
		Base:           commonpbutil.NewMsgBase(),
		DbName:         dbName,
		CollectionName: httpReq.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListRestoreSnapshotJobs", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListRestoreSnapshotJobs(reqCtx, req.(*milvuspb.ListRestoreSnapshotJobsRequest))
	})
	if err == nil {
		jobs := resp.(*milvuspb.ListRestoreSnapshotJobsResponse).GetJobs()
		records := make([]gin.H, 0, len(jobs))
		for _, info := range jobs {
			records = append(records, restoreSnapshotJobToREST(info))
		}
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"records": records},
		})
	}
	return resp, err
}

func (h *HandlersV2) refreshExternalCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RefreshExternalCollectionReq)
	req := &milvuspb.RefreshExternalCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		ExternalSource: httpReq.ExternalSource,
		ExternalSpec:   httpReq.ExternalSpec,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RefreshExternalCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RefreshExternalCollection(reqCtx, req.(*milvuspb.RefreshExternalCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"jobId": resp.(*milvuspb.RefreshExternalCollectionResponse).GetJobId()},
		})
	}
	return resp, err
}

func (h *HandlersV2) commitImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	jobIDGetter := anyReq.(JobIDGetter)
	jobID, err := strconv.ParseInt(jobIDGetter.GetJobID(), 10, 64)
	if err != nil {
		paramErr := merr.WrapErrParameterInvalid("int64 jobId", jobIDGetter.GetJobID(), err.Error())
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(paramErr), HTTPReturnMessage: paramErr.Error()})
		return nil, paramErr
	}
	if err := h.checkImportJobAuth(ctx, c, dbName, jobIDGetter.GetJobID()); err != nil {
		return nil, err
	}
	ctx = c.Request.Context()
	req := &datapb.CommitImportRequest{
		Base:  commonpbutil.NewMsgBase(),
		JobId: jobID,
	}
	c.Set(ContextRequest, req)
	// Internal DataCoord-only RPC; not exposed on MilvusService. Use the real
	// proto package in the trace/metric label so it reflects the actual method.
	resp, err := wrapperProxy(ctx, c, req, false, false,
		"/milvus.proto.data.DataCoord/CommitImport",
		func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CommitImport(reqCtx, req.(*datapb.CommitImportRequest))
		})
	if err == nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{}})
	}
	return resp, err
}

func (h *HandlersV2) getRefreshExternalCollectionProgress(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RefreshExternalCollectionProgressReq)
	req := &milvuspb.GetRefreshExternalCollectionProgressRequest{
		JobId: httpReq.JobID,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/GetRefreshExternalCollectionProgress", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetRefreshExternalCollectionProgress(reqCtx, req.(*milvuspb.GetRefreshExternalCollectionProgressRequest))
	})
	if err == nil {
		jobInfo := resp.(*milvuspb.GetRefreshExternalCollectionProgressResponse).GetJobInfo()
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: refreshExternalCollectionJobInfoToMap(jobInfo)})
	}
	return resp, err
}

func (h *HandlersV2) abortImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	jobIDGetter := anyReq.(JobIDGetter)
	jobID, err := strconv.ParseInt(jobIDGetter.GetJobID(), 10, 64)
	if err != nil {
		paramErr := merr.WrapErrParameterInvalid("int64 jobId", jobIDGetter.GetJobID(), err.Error())
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(paramErr), HTTPReturnMessage: paramErr.Error()})
		return nil, paramErr
	}
	if err := h.checkImportJobAuth(ctx, c, dbName, jobIDGetter.GetJobID()); err != nil {
		return nil, err
	}
	ctx = c.Request.Context()
	req := &datapb.AbortImportRequest{
		Base:  commonpbutil.NewMsgBase(),
		JobId: jobID,
	}
	c.Set(ContextRequest, req)
	// Internal DataCoord-only RPC; not exposed on MilvusService.
	resp, err := wrapperProxy(ctx, c, req, false, false,
		"/milvus.proto.data.DataCoord/AbortImport",
		func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.AbortImport(reqCtx, req.(*datapb.AbortImportRequest))
		})
	if err == nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{}})
	}
	return resp, err
}

func (h *HandlersV2) listRefreshExternalCollectionJobs(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*OptionalCollectionNameReq)
	req := &milvuspb.ListRefreshExternalCollectionJobsRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ListRefreshExternalCollectionJobs", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListRefreshExternalCollectionJobs(reqCtx, req.(*milvuspb.ListRefreshExternalCollectionJobsRequest))
	})
	if err == nil {
		response := resp.(*milvuspb.ListRefreshExternalCollectionJobsResponse)
		records := make([]map[string]interface{}, 0, len(response.GetJobs()))
		for _, jobInfo := range response.GetJobs() {
			records = append(records, refreshExternalCollectionJobInfoToMap(jobInfo))
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{"records": records}})
	}
	return resp, err
}

func refreshExternalCollectionJobInfoToMap(jobInfo *milvuspb.RefreshExternalCollectionJobInfo) map[string]interface{} {
	detail := make(map[string]interface{})
	if jobInfo == nil {
		return detail
	}
	detail["jobId"] = jobInfo.GetJobId()
	detail["collectionName"] = jobInfo.GetCollectionName()
	detail["state"] = jobInfo.GetState().String()
	detail["progress"] = jobInfo.GetProgress()
	detail["externalSource"] = jobInfo.GetExternalSource()
	detail["externalSpec"] = jobInfo.GetExternalSpec()
	detail["startTime"] = jobInfo.GetStartTime()
	detail["endTime"] = jobInfo.GetEndTime()
	if jobInfo.GetReason() != "" {
		detail["reason"] = jobInfo.GetReason()
	}
	return detail
}

func (h *HandlersV2) GetCollectionSchema(ctx context.Context, c *gin.Context, dbName, collectionName string) (*schemapb.CollectionSchema, error) {
	collSchema, err := proxy.GetCachedCollectionSchema(ctx, dbName, collectionName)
	if err == nil {
		return collSchema.CollectionSchema, nil
	}
	descReq := &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	descResp, err := wrapperProxy(ctx, c, descReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeCollection", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})
	if err != nil {
		return nil, err
	}
	response, _ := descResp.(*milvuspb.DescribeCollectionResponse)
	return response.Schema, nil
}

func (h *HandlersV2) getSegmentsInfo(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*GetSegmentsInfoReq)
	req := &internalpb.GetSegmentsInfoRequest{
		DbName:       httpReq.GetDbName(),
		CollectionID: httpReq.GetCollectionID(),
		SegmentIDs:   httpReq.GetSegmentIDs(),
	}

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetSegmentsInfo", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetSegmentsInfo(reqCtx, req.(*internalpb.GetSegmentsInfoRequest))
	})

	getLogs := func(binlogs []*internalpb.FieldBinlog) []interface{} {
		logIDs := make([]interface{}, 0, len(binlogs))
		for _, binlog := range binlogs {
			details := make(map[string]interface{})
			details["fieldID"] = binlog.GetFieldID()
			details["logIDs"] = binlog.GetLogIDs()
			logIDs = append(logIDs, details)
		}
		return logIDs
	}
	if err == nil {
		response := resp.(*internalpb.GetSegmentsInfoResponse)
		returnData := make(map[string]interface{})
		infos := make([]map[string]interface{}, 0)
		for _, segInfo := range response.GetSegmentInfos() {
			info := make(map[string]interface{})
			info["segmentID"] = segInfo.GetSegmentID()
			info["collectionID"] = segInfo.GetCollectionID()
			info["partitionID"] = segInfo.GetPartitionID()
			info["vChannel"] = segInfo.GetVChannel()
			info["numRows"] = segInfo.GetNumRows()
			info["state"] = segInfo.GetState()
			info["level"] = segInfo.GetLevel()
			info["isSorted"] = segInfo.GetIsSorted()
			info["insertLogs"] = getLogs(segInfo.GetInsertLogs())
			info["deltaLogs"] = getLogs(segInfo.GetDeltaLogs())
			info["statsLogs"] = getLogs(segInfo.GetStatsLogs())
			infos = append(infos, info)
		}
		returnData["segmentInfos"] = infos
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) getQuotaMetrics(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &internalpb.GetQuotaMetricsRequest{}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetQuotaMetrics", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetQuotaMetrics(reqCtx, req.(*internalpb.GetQuotaMetricsRequest))
	})
	if err == nil {
		response := resp.(*internalpb.GetQuotaMetricsResponse)
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: response.GetMetricsInfo()})
	}

	return resp, err
}

func (h *HandlersV2) runAnalyzer(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RunAnalyzerReq)

	// Convert text strings to byte slices
	placeholder := make([][]byte, len(httpReq.Text))
	for i, text := range httpReq.Text {
		placeholder[i] = []byte(text)
	}

	req := &milvuspb.RunAnalyzerRequest{
		DbName:         dbName,
		AnalyzerParams: httpReq.AnalyzerParams,
		Placeholder:    placeholder,
		WithDetail:     httpReq.WithDetail,
		WithHash:       httpReq.WithHash,
		CollectionName: httpReq.CollectionName,
		FieldName:      httpReq.FieldName,
		AnalyzerNames:  httpReq.AnalyzerNames,
	}

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RunAnalyzer", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RunAnalyzer(reqCtx, req.(*milvuspb.RunAnalyzerRequest))
	})

	if err == nil {
		analyzerResp := resp.(*milvuspb.RunAnalyzerResponse)
		// Convert response to HTTP-friendly format
		results := make([]gin.H, len(analyzerResp.Results))
		for i, result := range analyzerResp.Results {
			tokens := make([]gin.H, len(result.Tokens))
			for j, token := range result.Tokens {
				tokenData := gin.H{
					"token": token.Token,
				}
				if httpReq.WithDetail {
					tokenData["startOffset"] = token.StartOffset
					tokenData["endOffset"] = token.EndOffset
					tokenData["position"] = token.Position
					tokenData["positionLength"] = token.PositionLength
				}
				if httpReq.WithHash {
					tokenData["hash"] = token.Hash
				}
				tokens[j] = tokenData
			}
			results[i] = gin.H{
				"tokens": tokens,
			}
		}
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{
				"results": results,
			},
		})
	}

	return resp, err
}
