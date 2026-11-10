// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIndexSummaryReplacementAndEmptyCompletion(t *testing.T) {
	segment := &datapb.SegmentInfo{}
	old := &model.SegmentIndex{IndexID: 10, BuildID: 100, IndexFileKeys: []string{"index"}}
	ApplyIndexSummary(segment, old, 5, true)
	replacement := &model.SegmentIndex{IndexID: 10, BuildID: 200}
	ApplyIndexSummary(segment, replacement, 5, true)
	ApplyIndexSummary(segment, old, 5, false)
	require.Len(t, segment.IndexSummaries, 1)
	require.EqualValues(t, 200, segment.IndexSummaries[0].GetBuildId())
	require.False(t, segment.IndexSummaries[0].GetHasArtifact())
	ApplyIndexSummary(segment, replacement, 5, false)
	require.Empty(t, segment.IndexSummaries)
}
