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

package analyze

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type AnalyzeInspector struct {
	mt          *meta.Meta
	analyzeMeta *AnalyzeMeta

	scheduler task.GlobalScheduler
}

func NewAnalyzeInspector(ctx context.Context,
	mt *meta.Meta,
	analyzeMeta *AnalyzeMeta,
	scheduler task.GlobalScheduler,
) *AnalyzeInspector {
	return &AnalyzeInspector{
		mt:          mt,
		analyzeMeta: analyzeMeta,
		scheduler:   scheduler,
	}
}

func (ai *AnalyzeInspector) Start() {
	ai.reloadFromMeta()
}

func (ai *AnalyzeInspector) Stop() {
}

func (ai *AnalyzeInspector) reloadFromMeta() {
	analyzeTasks := ai.analyzeMeta.GetAllTasks()
	for _, t := range analyzeTasks {
		if t.GetState() != indexpb.JobState_JobStateInit &&
			t.GetState() != indexpb.JobState_JobStateRetry &&
			t.GetState() != indexpb.JobState_JobStateInProgress {
			continue
		}
		ai.scheduler.Enqueue(NewAnalyzeTask(
			proto.Clone(t).(*indexpb.AnalyzeTask),
			ai.mt,
			ai.analyzeMeta,
		))
	}
}
