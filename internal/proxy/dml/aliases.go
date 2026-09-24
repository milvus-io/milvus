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

package dml

import (
	"context"

	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Local aliases for the shared model types, declared here so the concrete task
// structs keep their original (embedded) names without importing the root
// proxy package. Follows the pChan/vChan precedent in DEPENDENCIES.md.
type (
	baseTask        = taskmodel.BaseTask
	Condition       = taskmodel.Condition
	schemaInfo      = metacache.SchemaInfo
	Timestamp       = typeutil.Timestamp
	UniqueID        = typeutil.UniqueID
	pChan           = taskmodel.PChan
	vChan           = taskmodel.VChan
	pChanStatistics = taskmodel.PChanStatistics
	Cache           = metacache.Cache
	tsoAllocator    = taskmodel.TsoAllocator
	BaseInsertTask  = taskmodel.BaseInsertTask
	collectionInfo  = metacache.CollectionInfo
	databaseInfo    = metacache.DatabaseInfo
	partitionInfo   = metacache.PartitionInfo
)

func NewTaskCondition(ctx context.Context) *taskmodel.TaskCondition {
	return taskmodel.NewTaskCondition(ctx)
}
