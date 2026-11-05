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

package compactor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestL0ProcessReturnsCheckedNoMatchTargets(t *testing.T) {
	task := &LevelZeroCompactionTask{BinlogIO: io.NewBinlogIO(nil), plan: &datapb.CompactionPlan{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}}}
	deletes := mockey.Mock(compaction.ComposeDeleteDataFromSegments).Return(storage.NewDeleteData([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)}, []uint64{100}), nil).Build()
	defer deletes.UnPatch()
	batch := mockey.Mock(getMaxBatchSize).Return(2).Build()
	defer batch.UnPatch()
	bloom := mockey.Mock((*LevelZeroCompactionTask).loadBF).To(func(_ *LevelZeroCompactionTask, _ context.Context, targets []*datapb.CompactionSegmentBinlogs) (map[int64]*pkoracle.BloomFilterSet, error) {
		out := make(map[int64]*pkoracle.BloomFilterSet)
		for _, target := range targets {
			out[target.GetSegmentID()] = pkoracle.NewBloomFilterSet()
		}
		return out, nil
	}).Build()
	defer bloom.UnPatch()
	write := mockey.Mock((*LevelZeroCompactionTask).splitAndWrite).To(func(_ *LevelZeroCompactionTask, _ context.Context, _ *storage.DeleteData, targets map[int64]*pkoracle.BloomFilterSet) ([]*datapb.CompactionSegment, error) {
		if _, exists := targets[20]; exists {
			return []*datapb.CompactionSegment{{SegmentID: 20, Deltalogs: []*datapb.FieldBinlog{{FieldID: 100}}}}, nil
		}
		return nil, nil
	}).Build()
	defer write.UnPatch()
	results, err := task.process(t.Context(), 0, []*datapb.CompactionSegmentBinlogs{{SegmentID: 20}, {SegmentID: 21}, {SegmentID: 22}}, nil)
	require.NoError(t, err)
	byID := make(map[int64]*datapb.CompactionSegment)
	for _, result := range results {
		byID[result.GetSegmentID()] = result
	}
	require.Len(t, byID, 3)
	require.NotEmpty(t, byID[20].GetDeltalogs())
	require.Empty(t, byID[21].GetDeltalogs())
	require.Empty(t, byID[22].GetDeltalogs())
	write.UnPatch()
	failure := merr.WrapErrServiceUnavailableMsg("injected delta write failure")
	failed := mockey.Mock((*LevelZeroCompactionTask).splitAndWrite).Return(nil, failure).Build()
	defer failed.UnPatch()
	results, err = task.process(t.Context(), 0, []*datapb.CompactionSegmentBinlogs{{SegmentID: 20}}, nil)
	require.ErrorIs(t, err, failure)
	require.Empty(t, results, "failure must not certify no-match targets")
}
