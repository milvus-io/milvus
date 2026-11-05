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

//go:build test

package compaction

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func NewSegmentChangeTestInspector(t *testing.T, m *meta.Meta, alloc allocator.Allocator) *Inspector {
	t.Helper()
	tasks, err := ctask.NewCompactionTaskMeta(t.Context(), m.Catalog)
	require.NoError(t, err)
	return NewCompactionInspector(m, tasks, nil, nil, nil, nil, nil, alloc, nil, nil, nil, index.NewTestVersionManager())
}

func (c *Inspector) CreateCompactTaskForTest(t *datapb.CompactionTask) (ctask.CompactionTask, error) {
	return c.createCompactTask(t)
}
func (c *Inspector) EnqueueForTest(t ctask.CompactionTask) error { return c.queueTasks.Enqueue(t) }
func (c *Inspector) ScheduleForTest() []ctask.CompactionTask     { return c.schedule() }
func (c *Inspector) QueueLenForTest() int                        { return c.queueTasks.Len() }
func (c *Inspector) CleaningForTest(id int64) bool               { _, ok := c.cleaningTasks[id]; return ok }
func (c *Inspector) CleanFailedForTest()                         { c.cleanFailedTasks() }
func (c *Inspector) L0ExclusionsForTest() typeutil.Set[string]   { return c.segmentChangeL0Exclusions() }
