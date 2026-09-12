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
	"context"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
)

// Aliases to the extracted task model, kept so the concrete task structs that
// remain in this package can keep their original (unexported) type names.

type (
	baseTask        = taskmodel.BaseTask
	dmlTask         = taskmodel.DMLTask
	tsoAllocator    = taskmodel.TsoAllocator
	Condition       = taskmodel.Condition
	TaskCondition   = taskmodel.TaskCondition
	pChan           = taskmodel.PChan
	vChan           = taskmodel.VChan
	pChanStatistics = taskmodel.PChanStatistics
	BaseInsertTask  = taskmodel.BaseInsertTask
)

func NewTaskCondition(ctx context.Context) *taskmodel.TaskCondition {
	return taskmodel.NewTaskCondition(ctx)
}
