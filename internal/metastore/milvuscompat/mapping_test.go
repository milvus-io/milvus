package milvuscompat

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
)

func TestMilvusCatalogMappingDocumentsEveryMethod(t *testing.T) {
	for _, tc := range []struct {
		name    string
		iface   reflect.Type
		mapping map[string]string
	}{
		{name: "RootCoordCatalog", iface: reflect.TypeOf((*metastore.RootCoordCatalog)(nil)).Elem(), mapping: rootCoordCatalogMapping},
		{name: "DataCoordCatalog", iface: reflect.TypeOf((*metastore.DataCoordCatalog)(nil)).Elem(), mapping: dataCoordCatalogMapping},
		{name: "QueryCoordCatalog", iface: reflect.TypeOf((*metastore.QueryCoordCatalog)(nil)).Elem(), mapping: queryCoordCatalogMapping},
		{name: "StreamingCoordCatalog", iface: reflect.TypeOf((*metastore.StreamingCoordCatalog)(nil)).Elem(), mapping: streamingCoordCatalogMapping},
		{name: "StreamingNodeCatalog", iface: reflect.TypeOf((*metastore.StreamingNodeCatalog)(nil)).Elem(), mapping: streamingNodeCatalogMapping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			methods := make(map[string]struct{}, tc.iface.NumMethod())
			for i := 0; i < tc.iface.NumMethod(); i++ {
				methods[tc.iface.Method(i).Name] = struct{}{}
			}

			var missing []string
			for method := range methods {
				target, ok := tc.mapping[method]
				if !ok || target == "" {
					missing = append(missing, method)
				}
			}
			sort.Strings(missing)
			require.Empty(t, missing)

			var unexpected []string
			for method := range tc.mapping {
				if _, ok := methods[method]; !ok {
					unexpected = append(unexpected, method)
				}
			}
			sort.Strings(unexpected)
			require.Empty(t, unexpected)
		})
	}
}

var rootCoordCatalogMapping = map[string]string{
	"CreateDatabase":              "Metadata().Databases().Create",
	"DropDatabase":                "Metadata().Databases().Drop",
	"ListDatabases":               "Metadata().Databases().List",
	"AlterDatabase":               "Metadata().Databases().Alter",
	"CreateCollection":            "Metadata().Collections().Create",
	"GetCollectionByID":           "Metadata().Collections().Get by collection ID",
	"GetCollectionByName":         "Metadata().Collections().Get by collection name",
	"ListCollections":             "Metadata().Collections().List",
	"CollectionExists":            "Metadata().Collections().Exists; error collapses to false for old bool API",
	"DropCollection":              "Metadata().Collections().Drop",
	"AlterCollection":             "Metadata().Collections().Alter",
	"AlterCollectionDB":           "Metadata().Collections().Alter (DBID change branch)",
	"CreatePartition":             "Metadata().Partitions().Create",
	"DropPartition":               "Metadata().Partitions().Drop",
	"AlterPartition":              "Metadata().Partitions().Alter",
	"CreateAlias":                 "Metadata().Aliases().Create",
	"DropAlias":                   "Metadata().Aliases().Drop",
	"AlterAlias":                  "Metadata().Aliases().Alter",
	"ListAliases":                 "Metadata().Aliases().List",
	"GetCredential":               "AccessControl().Credentials().Get",
	"AlterCredential":             "AccessControl().Credentials().Alter",
	"DropCredential":              "AccessControl().Credentials().Drop",
	"ListCredentials":             "AccessControl().Credentials().List",
	"ListCredentialsWithPasswd":   "AccessControl().Credentials().ListWithPasswords",
	"CreateRole":                  "AccessControl().Roles().Create",
	"DropRole":                    "AccessControl().Roles().Drop",
	"AlterUserRole":               "AccessControl().Roles().AlterUserRole",
	"ListRole":                    "AccessControl().Roles().List",
	"ListUser":                    "AccessControl().Roles().ListUser",
	"ListUserRole":                "AccessControl().Roles().ListUserRole",
	"AlterGrant":                  "AccessControl().Grants().Alter",
	"DeleteGrant":                 "AccessControl().Grants().DeleteByRole",
	"ListGrant":                   "AccessControl().Grants().List",
	"ListPolicy":                  "AccessControl().Grants().ListPolicy",
	"DeleteGrantByCollectionName": "AccessControl().Grants().DeleteByCollectionName",
	"MigrateGrantCollectionName":  "AccessControl().Grants().MigrateCollectionName",
	"BackupRBAC":                  "AccessControl().Grants().Backup",
	"RestoreRBAC":                 "AccessControl().Grants().Restore",
	"GetPrivilegeGroup":           "AccessControl().PrivilegeGroups().Get",
	"DropPrivilegeGroup":          "AccessControl().PrivilegeGroups().Drop",
	"SavePrivilegeGroup":          "AccessControl().PrivilegeGroups().Save",
	"ListPrivilegeGroups":         "AccessControl().PrivilegeGroups().List",
	"SaveFileResource":            "Metadata().Files().Save",
	"RemoveFileResource":          "Metadata().Files().Remove",
	"ListFileResource":            "Metadata().Files().List",
	"Close":                       "Catalog.Close",
}

var dataCoordCatalogMapping = map[string]string{
	"ListSegments":                       "Metadata().Segments().List",
	"AddSegment":                         "Metadata().Segments().Save",
	"AlterSegments":                      "Metadata().Segments().UpdateBatch",
	"SaveDroppedSegmentsInBatch":         "Metadata().Segments().MarkDropped",
	"DropSegment":                        "Metadata().Segments().Drop",
	"MarkChannelAdded":                   "InternalState().DataCoord().MarkChannelAdded",
	"MarkChannelDeleted":                 "InternalState().DataCoord().MarkChannelDeleted",
	"ShouldDropChannel":                  "InternalState().DataCoord().ShouldDropChannel; error collapses to false for old bool API",
	"ChannelExists":                      "InternalState().DataCoord().ChannelExists; error collapses to false for old bool API",
	"DropChannel":                        "InternalState().DataCoord().DropChannel",
	"ListChannelCheckpoint":              "InternalState().DataCoord().ListChannelCheckpoints",
	"SaveChannelCheckpoint":              "InternalState().DataCoord().SaveChannelCheckpoint",
	"SaveChannelCheckpoints":             "InternalState().DataCoord().SaveChannelCheckpoints",
	"DropChannelCheckpoint":              "InternalState().DataCoord().DropChannelCheckpoint",
	"CreateIndex":                        "Metadata().Indexes().CreateIndex",
	"ListIndexes":                        "Metadata().Indexes().ListIndexes",
	"AlterIndexes":                       "Metadata().Indexes().AlterIndexes",
	"DropIndex":                          "Metadata().Indexes().DropIndex",
	"CreateSegmentIndex":                 "Metadata().Indexes().SaveSegmentIndex",
	"ListSegmentIndexes":                 "Metadata().Indexes().ListSegmentIndexes",
	"AlterSegmentIndexes":                "Metadata().Indexes().AlterSegmentIndexes",
	"DropSegmentIndex":                   "Metadata().Indexes().DropSegmentIndex",
	"SaveImportJob":                      "InternalState().DataCoord().SaveImportJob",
	"ListImportJobs":                     "InternalState().DataCoord().ListImportJobs",
	"DropImportJob":                      "InternalState().DataCoord().DropImportJob",
	"SavePreImportTask":                  "InternalState().DataCoord().SavePreImportTask",
	"ListPreImportTasks":                 "InternalState().DataCoord().ListPreImportTasks",
	"DropPreImportTask":                  "InternalState().DataCoord().DropPreImportTask",
	"SaveImportTask":                     "InternalState().DataCoord().SaveImportTask",
	"ListImportTasks":                    "InternalState().DataCoord().ListImportTasks",
	"DropImportTask":                     "InternalState().DataCoord().DropImportTask",
	"SaveCopySegmentJob":                 "InternalState().DataCoord().SaveCopySegmentJob",
	"ListCopySegmentJobs":                "InternalState().DataCoord().ListCopySegmentJobs",
	"DropCopySegmentJob":                 "InternalState().DataCoord().DropCopySegmentJob",
	"SaveCopySegmentTask":                "InternalState().DataCoord().SaveCopySegmentTask",
	"SaveCopySegmentTasksBatch":          "InternalState().DataCoord().SaveCopySegmentTasksBatch",
	"ListCopySegmentTasks":               "InternalState().DataCoord().ListCopySegmentTasks",
	"DropCopySegmentTask":                "InternalState().DataCoord().DropCopySegmentTask",
	"GcConfirm":                          "InternalState().DataCoord().GcConfirm; error collapses to false for old bool API",
	"ListCompactionTask":                 "InternalState().DataCoord().ListCompactionTasks",
	"SaveCompactionTask":                 "InternalState().DataCoord().SaveCompactionTask",
	"DropCompactionTask":                 "InternalState().DataCoord().DropCompactionTask",
	"ListAnalyzeTasks":                   "InternalState().DataCoord().ListAnalyzeTasks",
	"SaveAnalyzeTask":                    "InternalState().DataCoord().SaveAnalyzeTask",
	"DropAnalyzeTask":                    "InternalState().DataCoord().DropAnalyzeTask",
	"ListPartitionStatsInfos":            "InternalState().DataCoord().ListPartitionStatsInfos",
	"SavePartitionStatsInfo":             "InternalState().DataCoord().SavePartitionStatsInfo",
	"DropPartitionStatsInfo":             "InternalState().DataCoord().DropPartitionStatsInfo",
	"SaveCurrentPartitionStatsVersion":   "InternalState().DataCoord().SaveCurrentPartitionStatsVersion",
	"GetCurrentPartitionStatsVersion":    "InternalState().DataCoord().GetCurrentPartitionStatsVersion",
	"DropCurrentPartitionStatsVersion":   "InternalState().DataCoord().DropCurrentPartitionStatsVersion",
	"ListStatsTasks":                     "InternalState().DataCoord().ListStatsTasks",
	"SaveStatsTask":                      "InternalState().DataCoord().SaveStatsTask",
	"DropStatsTask":                      "InternalState().DataCoord().DropStatsTask",
	"ListExternalCollectionRefreshJobs":  "InternalState().DataCoord().ListExternalCollectionRefreshJobs",
	"SaveExternalCollectionRefreshJob":   "InternalState().DataCoord().SaveExternalCollectionRefreshJob",
	"DropExternalCollectionRefreshJob":   "InternalState().DataCoord().DropExternalCollectionRefreshJob",
	"ListExternalCollectionRefreshTasks": "InternalState().DataCoord().ListExternalCollectionRefreshTasks",
	"SaveExternalCollectionRefreshTask":  "InternalState().DataCoord().SaveExternalCollectionRefreshTask",
	"DropExternalCollectionRefreshTask":  "InternalState().DataCoord().DropExternalCollectionRefreshTask",
	"SaveFileResource":                   "Metadata().Files().Save",
	"RemoveFileResource":                 "Metadata().Files().Remove",
	"ListFileResource":                   "Metadata().Files().List",
	"SaveSnapshot":                       "Metadata().Snapshots().Save",
	"DropSnapshot":                       "Metadata().Snapshots().Drop",
	"ListSnapshots":                      "Metadata().Snapshots().List",
}

var queryCoordCatalogMapping = map[string]string{
	"SaveCollection":          "InternalState().QueryCoord().SaveCollection",
	"SavePartition":           "InternalState().QueryCoord().SavePartition",
	"SaveReplica":             "InternalState().QueryCoord().SaveReplica",
	"GetCollections":          "InternalState().QueryCoord().GetCollections",
	"GetPartitions":           "InternalState().QueryCoord().GetPartitions",
	"GetReplicas":             "InternalState().QueryCoord().GetReplicas",
	"ReleaseCollection":       "InternalState().QueryCoord().ReleaseCollection",
	"ReleasePartition":        "InternalState().QueryCoord().ReleasePartition",
	"ReleaseReplicas":         "InternalState().QueryCoord().ReleaseReplicas",
	"ReleaseReplica":          "InternalState().QueryCoord().ReleaseReplica",
	"SaveResourceGroup":       "InternalState().QueryCoord().SaveResourceGroup",
	"RemoveResourceGroup":     "InternalState().QueryCoord().RemoveResourceGroup",
	"GetResourceGroups":       "InternalState().QueryCoord().GetResourceGroups",
	"SaveCollectionTargets":   "InternalState().QueryCoord().SaveCollectionTargets",
	"RemoveCollectionTarget":  "InternalState().QueryCoord().RemoveCollectionTarget",
	"RemoveCollectionTargets": "InternalState().QueryCoord().RemoveCollectionTargets",
	"GetCollectionTargets":    "InternalState().QueryCoord().GetCollectionTargets",
}

var streamingCoordCatalogMapping = map[string]string{
	"GetCChannel":                "InternalState().Streaming().GetCChannel",
	"SaveCChannel":               "InternalState().Streaming().SaveCChannel",
	"GetVersion":                 "InternalState().Streaming().GetVersion",
	"SaveVersion":                "InternalState().Streaming().SaveVersion",
	"ListPChannel":               "InternalState().Streaming().ListPChannels",
	"SavePChannels":              "InternalState().Streaming().SavePChannels",
	"ListBroadcastTask":          "InternalState().Streaming().ListBroadcastTasks",
	"SaveBroadcastTask":          "InternalState().Streaming().SaveBroadcastTask",
	"SaveReplicateConfiguration": "InternalState().Streaming().SaveReplicateConfiguration",
	"GetReplicateConfiguration":  "InternalState().Streaming().GetReplicateConfiguration",
}

var streamingNodeCatalogMapping = map[string]string{
	"ListVChannel":           "InternalState().Streaming().ListVChannels",
	"SaveVChannels":          "InternalState().Streaming().SaveVChannels",
	"ListSegmentAssignment":  "InternalState().Streaming().ListSegmentAssignments",
	"SaveSegmentAssignments": "InternalState().Streaming().SaveSegmentAssignments",
	"GetConsumeCheckpoint":   "InternalState().Streaming().GetConsumeCheckpoint",
	"SaveConsumeCheckpoint":  "InternalState().Streaming().SaveConsumeCheckpoint",
	"SaveSalvageCheckpoint":  "InternalState().Streaming().SaveSalvageCheckpoint",
	"GetSalvageCheckpoint":   "InternalState().Streaming().GetSalvageCheckpoint",
}
