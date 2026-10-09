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

package ddl

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// The constructors below replicate the exact field sets the root package used
// to write when it built these tasks with struct literals, including whether a
// task carries a baseTask MetaCache. The host node is consumed through the
// taskmodel.TaskNode contract only; no root import is needed.

// NewCreateDatabaseTask constructs a CreateDatabaseTask.
func NewCreateDatabaseTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.CreateDatabaseRequest) *CreateDatabaseTask {
	return &CreateDatabaseTask{
		Condition:             NewTaskCondition(ctx),
		CreateDatabaseRequest: request,
		ctx:                   ctx,
		mixCoord:              node.MixCoord(),
	}
}

// NewDropDatabaseTask constructs a DropDatabaseTask.
func NewDropDatabaseTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DropDatabaseRequest) *DropDatabaseTask {
	return &DropDatabaseTask{
		baseTask:            baseTask{MetaCache: node.GetMetaCache()},
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		DropDatabaseRequest: request,
		mixCoord:            node.MixCoord(),
	}
}

// NewListDatabaseTask constructs a ListDatabaseTask.
func NewListDatabaseTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.ListDatabasesRequest) *ListDatabaseTask {
	return &ListDatabaseTask{
		Condition:            NewTaskCondition(ctx),
		ListDatabasesRequest: request,
		ctx:                  ctx,
		mixCoord:             node.MixCoord(),
	}
}

// NewAlterDatabaseTask constructs an AlterDatabaseTask.
func NewAlterDatabaseTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AlterDatabaseRequest) *AlterDatabaseTask {
	return &AlterDatabaseTask{
		Condition:            NewTaskCondition(ctx),
		AlterDatabaseRequest: request,
		ctx:                  ctx,
		mixCoord:             node.MixCoord(),
	}
}

// NewDescribeDatabaseTask constructs a DescribeDatabaseTask.
func NewDescribeDatabaseTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DescribeDatabaseRequest) *DescribeDatabaseTask {
	return &DescribeDatabaseTask{
		Condition:               NewTaskCondition(ctx),
		DescribeDatabaseRequest: request,
		ctx:                     ctx,
		mixCoord:                node.MixCoord(),
	}
}

// NewCreateCollectionTask constructs a CreateCollectionTask.
func NewCreateCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.CreateCollectionRequest) *CreateCollectionTask {
	return &CreateCollectionTask{
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		ctx:                     ctx,
		mixCoord:                node.MixCoord(),
	}
}

// NewDropCollectionTask constructs a DropCollectionTask.
func NewDropCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DropCollectionRequest) *DropCollectionTask {
	return &DropCollectionTask{
		baseTask:              baseTask{MetaCache: node.GetMetaCache()},
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		mixCoord:              node.MixCoord(),
		chMgr:                 node.ChMgr(),
	}
}

// NewTruncateCollectionTask constructs a TruncateCollectionTask.
func NewTruncateCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.TruncateCollectionRequest) *TruncateCollectionTask {
	return &TruncateCollectionTask{
		baseTask:                  baseTask{MetaCache: node.GetMetaCache()},
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		TruncateCollectionRequest: request,
		mixCoord:                  node.MixCoord(),
		chMgr:                     node.ChMgr(),
	}
}

// NewHasCollectionTask constructs a HasCollectionTask.
func NewHasCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.HasCollectionRequest) *HasCollectionTask {
	return &HasCollectionTask{
		baseTask:             baseTask{MetaCache: node.GetMetaCache()},
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		mixCoord:             node.MixCoord(),
	}
}

// NewDescribeCollectionTask constructs a DescribeCollectionTask.
func NewDescribeCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DescribeCollectionRequest) *DescribeCollectionTask {
	return &DescribeCollectionTask{
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		ctx:                       ctx,
		mixCoord:                  node.MixCoord(),
	}
}

// NewShowCollectionsTask constructs a ShowCollectionsTask.
func NewShowCollectionsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.ShowCollectionsRequest) *ShowCollectionsTask {
	return &ShowCollectionsTask{
		baseTask:               baseTask{MetaCache: node.GetMetaCache()},
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		mixCoord:               node.MixCoord(),
	}
}

// NewAddCollectionFieldTask constructs an AddCollectionFieldTask.
func NewAddCollectionFieldTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AddCollectionFieldRequest, oldSchema *schemapb.CollectionSchema) *AddCollectionFieldTask {
	return &AddCollectionFieldTask{
		Condition:                 NewTaskCondition(ctx),
		AddCollectionFieldRequest: request,
		ctx:                       ctx,
		mixCoord:                  node.MixCoord(),
		oldSchema:                 oldSchema,
	}
}

// NewAddCollectionStructFieldTask constructs an AddCollectionStructFieldTask.
func NewAddCollectionStructFieldTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AddCollectionStructFieldRequest, oldSchema *schemapb.CollectionSchema) *AddCollectionStructFieldTask {
	return &AddCollectionStructFieldTask{
		Condition:                       NewTaskCondition(ctx),
		AddCollectionStructFieldRequest: request,
		ctx:                             ctx,
		mixCoord:                        node.MixCoord(),
		oldSchema:                       oldSchema,
	}
}

// NewAlterCollectionSchemaTask constructs an AlterCollectionSchemaTask.
func NewAlterCollectionSchemaTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AlterCollectionSchemaRequest, oldSchema *schemapb.CollectionSchema, collectionProperties []*commonpb.KeyValuePair, checkVecIndexWithDataType func(name string, dataType, elementType schemapb.DataType) bool) *AlterCollectionSchemaTask {
	return &AlterCollectionSchemaTask{
		Condition:                    NewTaskCondition(ctx),
		AlterCollectionSchemaRequest: request,
		ctx:                          ctx,
		node:                         node,
		mixCoord:                     node.MixCoord(),
		oldSchema:                    oldSchema,
		collectionProperties:         collectionProperties,
		checkVecIndexWithDataType:    checkVecIndexWithDataType,
	}
}

// NewAlterCollectionTask constructs an AlterCollectionTask.
func NewAlterCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AlterCollectionRequest) *AlterCollectionTask {
	return &AlterCollectionTask{
		baseTask:               baseTask{MetaCache: node.GetMetaCache()},
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		AlterCollectionRequest: request,
		node:                   node,
		mixCoord:               node.MixCoord(),
	}
}

// NewAlterCollectionFunctionTask constructs an AlterCollectionFunctionTask.
func NewAlterCollectionFunctionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AlterCollectionFunctionRequest) *AlterCollectionFunctionTask {
	return &AlterCollectionFunctionTask{
		baseTask:                       baseTask{MetaCache: node.GetMetaCache()},
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		AlterCollectionFunctionRequest: request,
		mixCoord:                       node.MixCoord(),
	}
}

// NewAlterCollectionFieldTask constructs an AlterCollectionFieldTask.
func NewAlterCollectionFieldTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AlterCollectionFieldRequest) *AlterCollectionFieldTask {
	return &AlterCollectionFieldTask{
		baseTask:                    baseTask{MetaCache: node.GetMetaCache()},
		ctx:                         ctx,
		Condition:                   NewTaskCondition(ctx),
		AlterCollectionFieldRequest: request,
		mixCoord:                    node.MixCoord(),
	}
}

// NewCreatePartitionTask constructs a CreatePartitionTask.
func NewCreatePartitionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.CreatePartitionRequest) *CreatePartitionTask {
	return &CreatePartitionTask{
		baseTask:               baseTask{MetaCache: node.GetMetaCache()},
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		mixCoord:               node.MixCoord(),
	}
}

// NewDropPartitionTask constructs a DropPartitionTask.
func NewDropPartitionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DropPartitionRequest) *DropPartitionTask {
	return &DropPartitionTask{
		baseTask:             baseTask{MetaCache: node.GetMetaCache()},
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		mixCoord:             node.MixCoord(),
	}
}

// NewHasPartitionTask constructs a HasPartitionTask.
func NewHasPartitionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.HasPartitionRequest) *HasPartitionTask {
	return &HasPartitionTask{
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		ctx:                 ctx,
		mixCoord:            node.MixCoord(),
	}
}

// NewLoadPartitionsTask constructs a LoadPartitionsTask.
func NewLoadPartitionsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.LoadPartitionsRequest) *LoadPartitionsTask {
	return &LoadPartitionsTask{
		baseTask:              baseTask{MetaCache: node.GetMetaCache()},
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		mixCoord:              node.MixCoord(),
	}
}

// NewReleasePartitionsTask constructs a ReleasePartitionsTask.
func NewReleasePartitionsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.ReleasePartitionsRequest) *ReleasePartitionsTask {
	return &ReleasePartitionsTask{
		baseTask:                 baseTask{MetaCache: node.GetMetaCache()},
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		mixCoord:                 node.MixCoord(),
	}
}

// NewShowPartitionsTask constructs a ShowPartitionsTask.
func NewShowPartitionsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.ShowPartitionsRequest) *ShowPartitionsTask {
	return &ShowPartitionsTask{
		baseTask:              baseTask{MetaCache: node.GetMetaCache()},
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ShowPartitionsRequest: request,
		mixCoord:              node.MixCoord(),
	}
}

// NewLoadCollectionTask constructs a LoadCollectionTask.
func NewLoadCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.LoadCollectionRequest) *LoadCollectionTask {
	return &LoadCollectionTask{
		baseTask:              baseTask{MetaCache: node.GetMetaCache()},
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		mixCoord:              node.MixCoord(),
	}
}

// NewReleaseCollectionTask constructs a ReleaseCollectionTask.
func NewReleaseCollectionTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.ReleaseCollectionRequest) *ReleaseCollectionTask {
	return &ReleaseCollectionTask{
		baseTask:                 baseTask{MetaCache: node.GetMetaCache()},
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		mixCoord:                 node.MixCoord(),
	}
}

// NewCreateResourceGroupTask constructs a CreateResourceGroupTask.
func NewCreateResourceGroupTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.CreateResourceGroupRequest) *CreateResourceGroupTask {
	return &CreateResourceGroupTask{
		Condition:                  NewTaskCondition(ctx),
		CreateResourceGroupRequest: request,
		ctx:                        ctx,
		mixCoord:                   node.MixCoord(),
	}
}

// NewUpdateResourceGroupsTask constructs an UpdateResourceGroupsTask.
func NewUpdateResourceGroupsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.UpdateResourceGroupsRequest) *UpdateResourceGroupsTask {
	return &UpdateResourceGroupsTask{
		Condition:                   NewTaskCondition(ctx),
		UpdateResourceGroupsRequest: request,
		ctx:                         ctx,
		mixCoord:                    node.MixCoord(),
	}
}

// NewDropResourceGroupTask constructs a DropResourceGroupTask.
func NewDropResourceGroupTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DropResourceGroupRequest) *DropResourceGroupTask {
	return &DropResourceGroupTask{
		Condition:                NewTaskCondition(ctx),
		DropResourceGroupRequest: request,
		ctx:                      ctx,
		mixCoord:                 node.MixCoord(),
	}
}

// NewDescribeResourceGroupTask constructs a DescribeResourceGroupTask.
func NewDescribeResourceGroupTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DescribeResourceGroupRequest) *DescribeResourceGroupTask {
	return &DescribeResourceGroupTask{
		baseTask:                     baseTask{MetaCache: node.GetMetaCache()},
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		DescribeResourceGroupRequest: request,
		mixCoord:                     node.MixCoord(),
	}
}

// NewTransferNodeTask constructs a TransferNodeTask.
func NewTransferNodeTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.TransferNodeRequest) *TransferNodeTask {
	return &TransferNodeTask{
		Condition:           NewTaskCondition(ctx),
		TransferNodeRequest: request,
		ctx:                 ctx,
		mixCoord:            node.MixCoord(),
	}
}

// NewTransferReplicaTask constructs a TransferReplicaTask.
func NewTransferReplicaTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.TransferReplicaRequest) *TransferReplicaTask {
	return &TransferReplicaTask{
		baseTask:               baseTask{MetaCache: node.GetMetaCache()},
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		TransferReplicaRequest: request,
		mixCoord:               node.MixCoord(),
	}
}

// NewListResourceGroupsTask constructs a ListResourceGroupsTask.
func NewListResourceGroupsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.ListResourceGroupsRequest) *ListResourceGroupsTask {
	return &ListResourceGroupsTask{
		Condition:                 NewTaskCondition(ctx),
		ListResourceGroupsRequest: request,
		ctx:                       ctx,
		mixCoord:                  node.MixCoord(),
	}
}

// NewRunAnalyzerTask constructs a RunAnalyzerTask.
func NewRunAnalyzerTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.RunAnalyzerRequest) *RunAnalyzerTask {
	return &RunAnalyzerTask{
		baseTask:           baseTask{MetaCache: node.GetMetaCache()},
		ctx:                ctx,
		lb:                 node.LBPolicy(),
		Condition:          NewTaskCondition(ctx),
		RunAnalyzerRequest: request,
	}
}

// NewCreateAliasTask constructs a CreateAliasTask.
func NewCreateAliasTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.CreateAliasRequest) *CreateAliasTask {
	return &CreateAliasTask{
		baseTask:           baseTask{MetaCache: node.GetMetaCache()},
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateAliasRequest: request,
		mixCoord:           node.MixCoord(),
	}
}

// NewDropAliasTask constructs a DropAliasTask.
func NewDropAliasTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DropAliasRequest) *DropAliasTask {
	return &DropAliasTask{
		Condition:        NewTaskCondition(ctx),
		DropAliasRequest: request,
		ctx:              ctx,
		mixCoord:         node.MixCoord(),
	}
}

// NewAlterAliasTask constructs an AlterAliasTask.
func NewAlterAliasTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.AlterAliasRequest) *AlterAliasTask {
	return &AlterAliasTask{
		baseTask:          baseTask{MetaCache: node.GetMetaCache()},
		ctx:               ctx,
		Condition:         NewTaskCondition(ctx),
		AlterAliasRequest: request,
		mixCoord:          node.MixCoord(),
	}
}

// NewDescribeAliasTask constructs a DescribeAliasTask.
func NewDescribeAliasTask(ctx context.Context, node taskmodel.TaskNode, nodeID UniqueID, request *milvuspb.DescribeAliasRequest) *DescribeAliasTask {
	return &DescribeAliasTask{
		Condition:            NewTaskCondition(ctx),
		nodeID:               nodeID,
		DescribeAliasRequest: request,
		ctx:                  ctx,
		mixCoord:             node.MixCoord(),
	}
}

// NewListAliasesTask constructs a ListAliasesTask.
func NewListAliasesTask(ctx context.Context, node taskmodel.TaskNode, nodeID UniqueID, request *milvuspb.ListAliasesRequest) *ListAliasesTask {
	return &ListAliasesTask{
		baseTask:           baseTask{MetaCache: node.GetMetaCache()},
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		nodeID:             nodeID,
		ListAliasesRequest: request,
		mixCoord:           node.MixCoord(),
	}
}

// NewFlushTask constructs a FlushTask.
func NewFlushTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.FlushRequest) *FlushTask {
	return &FlushTask{
		baseTask:     baseTask{MetaCache: node.GetMetaCache()},
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		mixCoord:     node.MixCoord(),
		chMgr:        node.ChMgr(),
	}
}

// NewFlushAllTask constructs a FlushAllTask.
func NewFlushAllTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.FlushAllRequest) *FlushAllTask {
	return &FlushAllTask{
		Condition:       NewTaskCondition(ctx),
		FlushAllRequest: request,
		ctx:             ctx,
		mixCoord:        node.MixCoord(),
	}
}

// NewImportTask constructs an ImportTask.
func NewImportTask(ctx context.Context, node taskmodel.TaskNode, req *internalpb.ImportRequest, resp *internalpb.ImportResponse) *ImportTask {
	return &ImportTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		node:      node,
		mixCoord:  node.MixCoord(),
		resp:      resp,
	}
}

// NewCreateIndexTask constructs a CreateIndexTask.
func NewCreateIndexTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.CreateIndexRequest, checkVecIndexWithDataType func(name string, dataType, elementType schemapb.DataType) bool) *CreateIndexTask {
	return &CreateIndexTask{
		baseTask:                  baseTask{MetaCache: node.GetMetaCache()},
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		req:                       req,
		node:                      node,
		mixCoord:                  node.MixCoord(),
		checkVecIndexWithDataType: checkVecIndexWithDataType,
	}
}

// NewAlterIndexTask constructs an AlterIndexTask.
func NewAlterIndexTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.AlterIndexRequest) *AlterIndexTask {
	return &AlterIndexTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewDescribeIndexTask constructs a DescribeIndexTask.
func NewDescribeIndexTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DescribeIndexRequest) *DescribeIndexTask {
	return &DescribeIndexTask{
		baseTask:             baseTask{MetaCache: node.GetMetaCache()},
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		mixCoord:             node.MixCoord(),
	}
}

// NewGetIndexStatisticsTask constructs a GetIndexStatisticsTask.
func NewGetIndexStatisticsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.GetIndexStatisticsRequest) *GetIndexStatisticsTask {
	return &GetIndexStatisticsTask{
		baseTask:                  baseTask{MetaCache: node.GetMetaCache()},
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		GetIndexStatisticsRequest: request,
		mixCoord:                  node.MixCoord(),
	}
}

// NewDropIndexTask constructs a DropIndexTask.
func NewDropIndexTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.DropIndexRequest) *DropIndexTask {
	return &DropIndexTask{
		baseTask:         baseTask{MetaCache: node.GetMetaCache()},
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		mixCoord:         node.MixCoord(),
	}
}

// NewGetIndexBuildProgressTask constructs a GetIndexBuildProgressTask.
func NewGetIndexBuildProgressTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.GetIndexBuildProgressRequest) *GetIndexBuildProgressTask {
	return &GetIndexBuildProgressTask{
		baseTask:                     baseTask{MetaCache: node.GetMetaCache()},
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		GetIndexBuildProgressRequest: request,
		mixCoord:                     node.MixCoord(),
	}
}

// NewGetIndexStateTask constructs a GetIndexStateTask.
func NewGetIndexStateTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.GetIndexStateRequest) *GetIndexStateTask {
	return &GetIndexStateTask{
		baseTask:             baseTask{MetaCache: node.GetMetaCache()},
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		mixCoord:             node.MixCoord(),
	}
}

// NewCreateSnapshotTask constructs a CreateSnapshotTask.
func NewCreateSnapshotTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.CreateSnapshotRequest) *CreateSnapshotTask {
	return &CreateSnapshotTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewDropSnapshotTask constructs a DropSnapshotTask.
func NewDropSnapshotTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.DropSnapshotRequest) *DropSnapshotTask {
	return &DropSnapshotTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewDescribeSnapshotTask constructs a DescribeSnapshotTask.
func NewDescribeSnapshotTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.DescribeSnapshotRequest) *DescribeSnapshotTask {
	return &DescribeSnapshotTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewListSnapshotsTask constructs a ListSnapshotsTask.
func NewListSnapshotsTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.ListSnapshotsRequest) *ListSnapshotsTask {
	return &ListSnapshotsTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewRestoreSnapshotTask constructs a RestoreSnapshotTask.
func NewRestoreSnapshotTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.RestoreSnapshotRequest) *RestoreSnapshotTask {
	return &RestoreSnapshotTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewGetRestoreSnapshotStateTask constructs a GetRestoreSnapshotStateTask.
func NewGetRestoreSnapshotStateTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.GetRestoreSnapshotStateRequest) *GetRestoreSnapshotStateTask {
	return &GetRestoreSnapshotStateTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewListRestoreSnapshotJobsTask constructs a ListRestoreSnapshotJobsTask.
func NewListRestoreSnapshotJobsTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.ListRestoreSnapshotJobsRequest) *ListRestoreSnapshotJobsTask {
	return &ListRestoreSnapshotJobsTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewPinSnapshotDataTask constructs a PinSnapshotDataTask.
func NewPinSnapshotDataTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.PinSnapshotDataRequest) *PinSnapshotDataTask {
	return &PinSnapshotDataTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// NewUnpinSnapshotDataTask constructs an UnpinSnapshotDataTask.
func NewUnpinSnapshotDataTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.UnpinSnapshotDataRequest) *UnpinSnapshotDataTask {
	return &UnpinSnapshotDataTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// Result returns the task's mutation/status result after execution.
func (t *CreateDatabaseTask) Result() *commonpb.Status                               { return t.result }
func (t *DropDatabaseTask) Result() *commonpb.Status                                 { return t.result }
func (t *ListDatabaseTask) Result() *milvuspb.ListDatabasesResponse                  { return t.result }
func (t *AlterDatabaseTask) Result() *commonpb.Status                                { return t.result }
func (t *DescribeDatabaseTask) Result() *milvuspb.DescribeDatabaseResponse           { return t.result }
func (t *CreateCollectionTask) Result() *commonpb.Status                             { return t.result }
func (t *DropCollectionTask) Result() *commonpb.Status                               { return t.result }
func (t *TruncateCollectionTask) Result() *milvuspb.TruncateCollectionResponse       { return t.result }
func (t *HasCollectionTask) Result() *milvuspb.BoolResponse                          { return t.result }
func (t *DescribeCollectionTask) Result() *milvuspb.DescribeCollectionResponse       { return t.result }
func (t *ShowCollectionsTask) Result() *milvuspb.ShowCollectionsResponse             { return t.result }
func (t *AddCollectionFieldTask) Result() *commonpb.Status                           { return t.result }
func (t *AddCollectionFieldTask) OldSchema() *schemapb.CollectionSchema              { return t.oldSchema }
func (t *AddCollectionFieldTask) SetResult(status *commonpb.Status)                  { t.result = status }
func (t *AddCollectionStructFieldTask) Result() *commonpb.Status                     { return t.result }
func (t *AlterCollectionTask) Result() *commonpb.Status                              { return t.result }
func (t *AlterCollectionFunctionTask) Result() *commonpb.Status                      { return t.result }
func (t *AlterCollectionFieldTask) Result() *commonpb.Status                         { return t.result }
func (t *CreatePartitionTask) Result() *commonpb.Status                              { return t.result }
func (t *DropPartitionTask) Result() *commonpb.Status                                { return t.result }
func (t *HasPartitionTask) Result() *milvuspb.BoolResponse                           { return t.result }
func (t *ShowPartitionsTask) Result() *milvuspb.ShowPartitionsResponse               { return t.result }
func (t *LoadCollectionTask) Result() *commonpb.Status                               { return t.result }
func (t *ReleaseCollectionTask) Result() *commonpb.Status                            { return t.result }
func (t *LoadPartitionsTask) Result() *commonpb.Status                               { return t.result }
func (t *ReleasePartitionsTask) Result() *commonpb.Status                            { return t.result }
func (t *CreateResourceGroupTask) Result() *commonpb.Status                          { return t.result }
func (t *UpdateResourceGroupsTask) Result() *commonpb.Status                         { return t.result }
func (t *DropResourceGroupTask) Result() *commonpb.Status                            { return t.result }
func (t *DescribeResourceGroupTask) Result() *milvuspb.DescribeResourceGroupResponse { return t.result }
func (t *TransferNodeTask) Result() *commonpb.Status                                 { return t.result }
func (t *TransferReplicaTask) Result() *commonpb.Status                              { return t.result }
func (t *ListResourceGroupsTask) Result() *milvuspb.ListResourceGroupsResponse       { return t.result }
func (t *RunAnalyzerTask) Result() *milvuspb.RunAnalyzerResponse                     { return t.result }
func (t *CreateAliasTask) Result() *commonpb.Status                                  { return t.result }
func (t *DropAliasTask) Result() *commonpb.Status                                    { return t.result }
func (t *AlterAliasTask) Result() *commonpb.Status                                   { return t.result }
func (t *DescribeAliasTask) Result() *milvuspb.DescribeAliasResponse                 { return t.result }
func (t *ListAliasesTask) Result() *milvuspb.ListAliasesResponse                     { return t.result }
func (t *FlushTask) Result() *milvuspb.FlushResponse                                 { return t.result }
func (t *FlushAllTask) Result() *milvuspb.FlushAllResponse                           { return t.result }
func (t *ImportTask) Resp() *internalpb.ImportResponse                               { return t.resp }
func (t *ImportTask) Request() *internalpb.ImportRequest                             { return t.req }
func (t *CreateIndexTask) Result() *commonpb.Status                                  { return t.result }
func (t *CreateIndexTask) Request() *milvuspb.CreateIndexRequest                     { return t.req }
func (t *AlterIndexTask) Result() *commonpb.Status                                   { return t.result }
func (t *AlterIndexTask) Request() *milvuspb.AlterIndexRequest                       { return t.req }
func (t *DescribeIndexTask) Result() *milvuspb.DescribeIndexResponse                 { return t.result }
func (t *GetIndexStatisticsTask) Result() *milvuspb.GetIndexStatisticsResponse       { return t.result }
func (t *DropIndexTask) Result() *commonpb.Status                                    { return t.result }
func (t *GetIndexBuildProgressTask) Result() *milvuspb.GetIndexBuildProgressResponse { return t.result }
func (t *GetIndexStateTask) Result() *milvuspb.GetIndexStateResponse                 { return t.result }
func (t *CreateSnapshotTask) Result() *commonpb.Status                               { return t.result }
func (t *DropSnapshotTask) Result() *commonpb.Status                                 { return t.result }
func (t *DescribeSnapshotTask) Result() *milvuspb.DescribeSnapshotResponse           { return t.result }
func (t *ListSnapshotsTask) Result() *milvuspb.ListSnapshotsResponse                 { return t.result }
func (t *RestoreSnapshotTask) Result() *milvuspb.RestoreSnapshotResponse             { return t.result }
func (t *GetRestoreSnapshotStateTask) Result() *milvuspb.GetRestoreSnapshotStateResponse {
	return t.result
}
func (t *ListRestoreSnapshotJobsTask) Result() *milvuspb.ListRestoreSnapshotJobsResponse {
	return t.result
}
func (t *PinSnapshotDataTask) Result() *milvuspb.PinSnapshotDataResponse { return t.result }
func (t *UnpinSnapshotDataTask) Result() *commonpb.Status                { return t.result }

func (t *CreateSnapshotTask) Request() *milvuspb.CreateSnapshotRequest     { return t.req }
func (t *DropSnapshotTask) Request() *milvuspb.DropSnapshotRequest         { return t.req }
func (t *DescribeSnapshotTask) Request() *milvuspb.DescribeSnapshotRequest { return t.req }
func (t *ListSnapshotsTask) Request() *milvuspb.ListSnapshotsRequest       { return t.req }
func (t *RestoreSnapshotTask) Request() *milvuspb.RestoreSnapshotRequest   { return t.req }
func (t *GetRestoreSnapshotStateTask) Request() *milvuspb.GetRestoreSnapshotStateRequest {
	return t.req
}
func (t *ListRestoreSnapshotJobsTask) Request() *milvuspb.ListRestoreSnapshotJobsRequest {
	return t.req
}
func (t *PinSnapshotDataTask) Request() *milvuspb.PinSnapshotDataRequest     { return t.req }
func (t *UnpinSnapshotDataTask) Request() *milvuspb.UnpinSnapshotDataRequest { return t.req }

func (t *CreateSnapshotTask) SetResult(status *commonpb.Status)                   { t.result = status }
func (t *DropSnapshotTask) SetResult(status *commonpb.Status)                     { t.result = status }
func (t *DescribeSnapshotTask) SetResult(resp *milvuspb.DescribeSnapshotResponse) { t.result = resp }
func (t *ListSnapshotsTask) SetResult(resp *milvuspb.ListSnapshotsResponse)       { t.result = resp }
func (t *RestoreSnapshotTask) SetResult(resp *milvuspb.RestoreSnapshotResponse)   { t.result = resp }
