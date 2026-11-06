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

package bulkimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestJobIDRangesSet_NilCheckNotEmptyCheck(t *testing.T) {
	job := &Job{ImportJob: &datapb.ImportJob{Files: []*internalpb.ImportFile{
		{Id: 1, IdRange: &commonpb.IDRange{Begin: 5, End: 5}}, // empty range counts as set
		{Id: 2},
	}}}
	assert.False(t, JobIDRangesSet(job))
	job.GetFiles()[1].IdRange = &commonpb.IDRange{Begin: 5, End: 9}
	assert.True(t, JobIDRangesSet(job))
}

func TestUpdateJobIDRanges_SetsRangesAndPersists(t *testing.T) {
	ctx := context.Background()
	var saved []*datapb.ImportJob
	importMeta := NewCapturingImportMeta(t, &saved)

	job := &Job{
		ImportJob: &datapb.ImportJob{
			JobID: 1,
			State: internalpb.ImportJobState_PreImporting,
			Files: []*internalpb.ImportFile{{Id: 10}, {Id: 11}},
		},
		Tr: timerecord.NewTimeRecorder("test"),
	}
	require.NoError(t, importMeta.AddJob(ctx, job))

	ranges := []*commonpb.IDRange{
		{Begin: 100, End: 110},
		{Begin: 110, End: 110}, // zero-row file keeps its empty range
	}
	require.NoError(t, importMeta.UpdateJob(ctx, 1, UpdateJobIDRanges(ranges)))

	// Survives re-GetJob: ranges by position.
	got := importMeta.GetJob(ctx, 1)
	require.NotNil(t, got)
	assert.EqualValues(t, 100, got.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, got.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 110, got.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, got.GetFiles()[1].GetIdRange().GetEnd())

	// Persisted: the proto handed to the catalog carries the ranges.
	require.NotEmpty(t, saved)
	last := saved[len(saved)-1]
	assert.EqualValues(t, 100, last.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, last.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 110, last.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, last.GetFiles()[1].GetIdRange().GetEnd())
}
