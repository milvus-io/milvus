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

package task

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestL0QueuedPlanCannotDropHistoryOnSuspendedTarget(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	mt.L0CompactionSuspended = func(string) bool { return true }
	task := &L0CompactionTask{meta: mt}
	task.taskProto.Store(&datapb.CompactionTask{Channel: "target"})
	_, err = task.BuildCompactionRequest()
	require.Error(t, err, "guard runs before creating a worker or a fast-finish plan")
	require.Error(t, task.SaveSegmentMeta(nil), "already running worker cannot retire L0 after suspension")
}
