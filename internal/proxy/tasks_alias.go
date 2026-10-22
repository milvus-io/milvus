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

package proxy

import (
	"github.com/milvus-io/milvus/internal/proxy/ddl"
	"github.com/milvus-io/milvus/internal/proxy/dml"
	"github.com/milvus-io/milvus/internal/proxy/dql"
)

// Aliases to the extracted dql/dml packages, kept so the composition root can
// keep using the original (unexported) type names. This is the only place in
// the root package allowed to reference the dql/dml packages.

type (
	searchTask                  = dql.SearchTask
	queryTask                   = dql.QueryTask
	getStatisticsTask           = dql.GetStatisticsTask
	getCollectionStatisticsTask = dql.GetCollectionStatisticsTask
	getPartitionStatisticsTask  = dql.GetPartitionStatisticsTask

	insertTask              = dml.InsertTask
	upsertTask              = dml.UpsertTask
	deleteTask              = dml.DeleteTask
	deleteRunner            = dml.DeleteRunner
	batchUpdateManifestTask = dml.BatchUpdateManifestTask

	createDatabaseTask           = ddl.CreateDatabaseTask
	dropDatabaseTask             = ddl.DropDatabaseTask
	listDatabaseTask             = ddl.ListDatabaseTask
	alterDatabaseTask            = ddl.AlterDatabaseTask
	describeDatabaseTask         = ddl.DescribeDatabaseTask
	createCollectionTask         = ddl.CreateCollectionTask
	dropCollectionTask           = ddl.DropCollectionTask
	truncateCollectionTask       = ddl.TruncateCollectionTask
	hasCollectionTask            = ddl.HasCollectionTask
	describeCollectionTask       = ddl.DescribeCollectionTask
	showCollectionsTask          = ddl.ShowCollectionsTask
	addCollectionFieldTask       = ddl.AddCollectionFieldTask
	addCollectionStructFieldTask = ddl.AddCollectionStructFieldTask
	alterCollectionSchemaTask    = ddl.AlterCollectionSchemaTask
	alterCollectionTask          = ddl.AlterCollectionTask
	alterCollectionFunctionTask  = ddl.AlterCollectionFunctionTask
	alterCollectionFieldTask     = ddl.AlterCollectionFieldTask
	createPartitionTask          = ddl.CreatePartitionTask
	dropPartitionTask            = ddl.DropPartitionTask
	hasPartitionTask             = ddl.HasPartitionTask
	showPartitionsTask           = ddl.ShowPartitionsTask
	loadCollectionTask           = ddl.LoadCollectionTask
	releaseCollectionTask        = ddl.ReleaseCollectionTask
	loadPartitionsTask           = ddl.LoadPartitionsTask
	releasePartitionsTask        = ddl.ReleasePartitionsTask
	flushTask                    = ddl.FlushTask
	flushAllTask                 = ddl.FlushAllTask
	importTask                   = ddl.ImportTask
	createIndexTask              = ddl.CreateIndexTask
	alterIndexTask               = ddl.AlterIndexTask
	describeIndexTask            = ddl.DescribeIndexTask
	getIndexStatisticsTask       = ddl.GetIndexStatisticsTask
	dropIndexTask                = ddl.DropIndexTask
	getIndexBuildProgressTask    = ddl.GetIndexBuildProgressTask
	getIndexStateTask            = ddl.GetIndexStateTask
	createSnapshotTask           = ddl.CreateSnapshotTask
	dropSnapshotTask             = ddl.DropSnapshotTask
	describeSnapshotTask         = ddl.DescribeSnapshotTask
	listSnapshotsTask            = ddl.ListSnapshotsTask
	restoreSnapshotTask          = ddl.RestoreSnapshotTask
	getRestoreSnapshotStateTask  = ddl.GetRestoreSnapshotStateTask
	listRestoreSnapshotJobsTask  = ddl.ListRestoreSnapshotJobsTask
	pinSnapshotDataTask          = ddl.PinSnapshotDataTask
	unpinSnapshotDataTask        = ddl.UnpinSnapshotDataTask

	CreateResourceGroupTask   = ddl.CreateResourceGroupTask
	UpdateResourceGroupsTask  = ddl.UpdateResourceGroupsTask
	DropResourceGroupTask     = ddl.DropResourceGroupTask
	DescribeResourceGroupTask = ddl.DescribeResourceGroupTask
	TransferNodeTask          = ddl.TransferNodeTask
	TransferReplicaTask       = ddl.TransferReplicaTask
	ListResourceGroupsTask    = ddl.ListResourceGroupsTask
	RunAnalyzerTask           = ddl.RunAnalyzerTask
	CreateAliasTask           = ddl.CreateAliasTask
	DropAliasTask             = ddl.DropAliasTask
	AlterAliasTask            = ddl.AlterAliasTask
	DescribeAliasTask         = ddl.DescribeAliasTask
	ListAliasesTask           = ddl.ListAliasesTask
)

// Constants re-exported from the dql package so external consumers of the root
// proxy package (e.g. the REST httpserver) keep compiling. They are true consts
// because dql declares them as consts; a var alias would break callers that use
// them in compile-time constant contexts.
const (
	AnnsFieldKey           = dql.AnnsFieldKey
	AnalyzerKey            = dql.AnalyzerKey
	ExprParamsKey          = dql.ExprParamsKey
	GroupByFieldKey        = dql.GroupByFieldKey
	GroupByFieldsKey       = dql.GroupByFieldsKey
	GroupSizeKey           = dql.GroupSizeKey
	IgnoreGrowingKey       = dql.IgnoreGrowingKey
	IteratorField          = dql.IteratorField
	LimitKey               = dql.LimitKey
	MetricTypeKey          = dql.MetricTypeKey
	NQKey                  = dql.NQKey
	NormScoreKey           = dql.NormScoreKey
	OffsetKey              = dql.OffsetKey
	OrderByFieldsKey       = dql.OrderByFieldsKey
	ParamsKey              = dql.ParamsKey
	PipelineTraceKey       = dql.PipelineTraceKey
	QueryIterLastOffsetKey = dql.QueryIterLastOffsetKey
	QueryIterLastPKKey     = dql.QueryIterLastPKKey
	RRFParamsKey           = dql.RRFParamsKey
	RankTypeKey            = dql.RankTypeKey
	ReduceStopForBestKey   = dql.ReduceStopForBestKey
	RoundDecimalKey        = dql.RoundDecimalKey
	SearchIterBatchSizeKey = dql.SearchIterBatchSizeKey
	SearchIterIdKey        = dql.SearchIterIdKey
	SearchIterLastBoundKey = dql.SearchIterLastBoundKey
	StrictCastKey          = dql.StrictCastKey
	StrictGroupSize        = dql.StrictGroupSize
	TimefieldsKey          = dql.TimefieldsKey
	TopKKey                = dql.TopKKey
	WeightsParamsKey       = dql.WeightsParamsKey
	CollectionID           = dql.CollectionID

	CreateSnapshotTaskName = ddl.CreateSnapshotTaskName
)

var (
	NewSearchTask                            = dql.NewSearchTask
	NewQueryTask                             = dql.NewQueryTask
	NewGetStatisticsTask                     = dql.NewGetStatisticsTask
	NewGetCollectionStatisticsTask           = dql.NewGetCollectionStatisticsTask
	NewGetPartitionStatisticsTask            = dql.NewGetPartitionStatisticsTask
	ConvertHybridSearchToSearch              = dql.ConvertHybridSearchToSearch
	PickFieldData                            = dql.PickFieldData
	MatchCountRule                           = dql.MatchCountRule
	WrapPlanCreationError                    = dql.WrapPlanCreationError
	GetPartitionIDs                          = dql.GetPartitionIDs
	MarshalPlanWithMembershipFilterSizeLimit = dql.MarshalPlanWithMembershipFilterSizeLimit
	NormalizeFP32ToFP16BF16VectorFieldData   = dql.NormalizeFP32ToFP16BF16VectorFieldData
	FormatTimestamptzFields                  = dql.FormatTimestamptzFields
	NewInsertTask                            = dml.NewInsertTask
	NewDeleteRunner                          = dml.NewDeleteRunner
	NewUpsertTask                            = dml.NewUpsertTask
	NewBatchUpdateManifestTask               = dml.NewBatchUpdateManifestTask

	DescribeCollectionErrorStatus   = ddl.DescribeCollectionErrorStatus
	ProjectDescribeCollectionSchema = ddl.ProjectDescribeCollectionSchema
	DescribeCollectionRPCContext    = ddl.DescribeCollectionRPCContext

	NewCreateDatabaseTask           = ddl.NewCreateDatabaseTask
	NewDropDatabaseTask             = ddl.NewDropDatabaseTask
	NewListDatabaseTask             = ddl.NewListDatabaseTask
	NewAlterDatabaseTask            = ddl.NewAlterDatabaseTask
	NewDescribeDatabaseTask         = ddl.NewDescribeDatabaseTask
	NewCreateCollectionTask         = ddl.NewCreateCollectionTask
	NewDropCollectionTask           = ddl.NewDropCollectionTask
	NewTruncateCollectionTask       = ddl.NewTruncateCollectionTask
	NewHasCollectionTask            = ddl.NewHasCollectionTask
	NewDescribeCollectionTask       = ddl.NewDescribeCollectionTask
	NewShowCollectionsTask          = ddl.NewShowCollectionsTask
	NewAddCollectionFieldTask       = ddl.NewAddCollectionFieldTask
	NewAddCollectionStructFieldTask = ddl.NewAddCollectionStructFieldTask
	NewAlterCollectionSchemaTask    = ddl.NewAlterCollectionSchemaTask
	NewAlterCollectionTask          = ddl.NewAlterCollectionTask
	NewAlterCollectionFunctionTask  = ddl.NewAlterCollectionFunctionTask
	NewAlterCollectionFieldTask     = ddl.NewAlterCollectionFieldTask
	NewCreatePartitionTask          = ddl.NewCreatePartitionTask
	NewDropPartitionTask            = ddl.NewDropPartitionTask
	NewHasPartitionTask             = ddl.NewHasPartitionTask
	NewShowPartitionsTask           = ddl.NewShowPartitionsTask
	NewLoadCollectionTask           = ddl.NewLoadCollectionTask
	NewReleaseCollectionTask        = ddl.NewReleaseCollectionTask
	NewLoadPartitionsTask           = ddl.NewLoadPartitionsTask
	NewReleasePartitionsTask        = ddl.NewReleasePartitionsTask
	NewCreateResourceGroupTask      = ddl.NewCreateResourceGroupTask
	NewUpdateResourceGroupsTask     = ddl.NewUpdateResourceGroupsTask
	NewDropResourceGroupTask        = ddl.NewDropResourceGroupTask
	NewDescribeResourceGroupTask    = ddl.NewDescribeResourceGroupTask
	NewTransferNodeTask             = ddl.NewTransferNodeTask
	NewTransferReplicaTask          = ddl.NewTransferReplicaTask
	NewListResourceGroupsTask       = ddl.NewListResourceGroupsTask
	NewRunAnalyzerTask              = ddl.NewRunAnalyzerTask
	NewCreateAliasTask              = ddl.NewCreateAliasTask
	NewDropAliasTask                = ddl.NewDropAliasTask
	NewAlterAliasTask               = ddl.NewAlterAliasTask
	NewDescribeAliasTask            = ddl.NewDescribeAliasTask
	NewListAliasesTask              = ddl.NewListAliasesTask
	NewFlushTask                    = ddl.NewFlushTask
	NewFlushAllTask                 = ddl.NewFlushAllTask
	NewImportTask                   = ddl.NewImportTask
	NewCreateIndexTask              = ddl.NewCreateIndexTask
	NewAlterIndexTask               = ddl.NewAlterIndexTask
	NewDescribeIndexTask            = ddl.NewDescribeIndexTask
	NewGetIndexStatisticsTask       = ddl.NewGetIndexStatisticsTask
	NewDropIndexTask                = ddl.NewDropIndexTask
	NewGetIndexBuildProgressTask    = ddl.NewGetIndexBuildProgressTask
	NewGetIndexStateTask            = ddl.NewGetIndexStateTask
	NewCreateSnapshotTask           = ddl.NewCreateSnapshotTask
	NewDropSnapshotTask             = ddl.NewDropSnapshotTask
	NewDescribeSnapshotTask         = ddl.NewDescribeSnapshotTask
	NewListSnapshotsTask            = ddl.NewListSnapshotsTask
	NewRestoreSnapshotTask          = ddl.NewRestoreSnapshotTask
	NewGetRestoreSnapshotStateTask  = ddl.NewGetRestoreSnapshotStateTask
	NewListRestoreSnapshotJobsTask  = ddl.NewListRestoreSnapshotJobsTask
	NewPinSnapshotDataTask          = ddl.NewPinSnapshotDataTask
	NewUnpinSnapshotDataTask        = ddl.NewUnpinSnapshotDataTask
)
