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
	"math"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func (s *ClusteringCompactionTaskSuite) prepareFloatingClusteringTask(dataType schemapb.DataType, values []interface{}) {
	schema := genCollectionSchema()
	schema.Fields[3].DataType = dataType
	schema.Fields[3].Nullable = true
	writer, err := NewSegmentWriter(schema, 1000, compactionBatchSize, 1001, PartitionID, CollectionID, nil)
	s.Require().NoError(err)
	for i, value := range values {
		row := genRow(int64(i))
		row[101] = value
		s.Require().NoError(writer.Write(&storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			Value:     row,
		}))
	}
	writer.FlushAndIsFull()
	kvs, logs, err := serializeWrite(context.Background(), s.mockAlloc, writer)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, paths []string) ([][]byte, error) {
		result := make([][]byte, 0, len(paths))
		for _, path := range paths {
			result = append(result, kvs[path])
		}
		return result, nil
	})
	s.plan.Schema = schema
	s.plan.ClusteringKeyField = 101
	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{{CollectionID: CollectionID, SegmentID: 1001, FieldBinlogs: lo.Values(logs)}}
	s.plan.PreferSegmentRows = 2
	s.plan.MaxSegmentRows = 4
	s.plan.MaxSize = 1 << 30
	s.plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 1, End: 101}
	s.task.compactionParams, err = compaction.ParseParamsFromJSON(s.plan.JsonParams)
	s.Require().NoError(err)
	s.Require().NoError(s.task.init())
}

func (s *ClusteringCompactionTaskSuite) TestFloatingClusteringNaNBuffersAndMapping() {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		s.Run(dataType.String(), func() {
			var values []interface{}
			for _, value := range []float64{math.NaN(), math.Float64frombits(0xfff8000000000042), math.NaN(), math.Copysign(0, -1), 0, math.Inf(-1), math.Inf(1), -2, 2} {
				if dataType == schemapb.DataType_Float {
					values = append(values, float32(value))
				} else {
					values = append(values, value)
				}
			}
			values = append(values, nil)
			s.prepareFloatingClusteringTask(dataType, values)
			defer s.task.cleanUp(context.Background())
			analyzed, err := s.task.scalarAnalyze(context.Background())
			s.Require().NoError(err)
			s.Len(analyzed, 7, "NaNs and signed zeros each form one bucket; NULL is separate")
			s.EqualValues(3, analyzed[scalarClusteringMapKey(values[0])].count)
			s.EqualValues(2, analyzed[scalarClusteringMapKey(values[3])].count)

			// Exercise the production analyzer-to-buffer path, not just key helpers.
			s.Require().NoError(s.task.getScalarAnalyzeResult(context.Background()))
			nanBuffer := s.task.keyToBufferFunc(values[0])
			s.Require().NotNil(nanBuffer)
			s.Same(nanBuffer, s.task.keyToBufferFunc(values[1]))
			s.Same(nanBuffer, s.task.keyToBufferFunc(values[2]))
			s.Same(s.task.keyToBufferFunc(values[3]), s.task.keyToBufferFunc(values[4]))
			s.Require().NotNil(s.task.keyToBufferFunc(nil))
			s.NotSame(nanBuffer, s.task.keyToBufferFunc(nil))
			s.True(nanBuffer.clusteringKeyFieldStats.Max.EQ(storage.NewScalarFieldValue(dataType, values[0])))

			// mapping invokes ClusterBuffer.Write for every raw source value, including
			// different NaN payloads, so this catches the original nil-buffer panic.
			outputs, snapshot, err := s.task.mapping(context.Background())
			s.Require().NoError(err)
			s.EqualValues(len(values), lo.SumBy(outputs, func(segment *datapb.CompactionSegment) int64 { return segment.NumOfRows }))
			encoded, err := storage.SerializePartitionStatsSnapshot(snapshot)
			s.Require().NoError(err)
			restored, err := storage.DeserializePartitionsStatsSnapshot(encoded)
			s.Require().NoError(err)
			s.Less(len(restored.SegmentStats), len(snapshot.SegmentStats), "nonfinite bounds must be omitted from legacy JSON")
		})
	}
}

func (s *ClusteringCompactionTaskSuite) TestFloatingClusteringCoordinatorCapability() {
	for _, test := range []struct {
		name        string
		values      []interface{}
		unsupported bool
	}{
		{"finite", []interface{}{float64(-1), float64(1)}, false},
		{"nan", []interface{}{math.NaN(), float64(1)}, true},
		{"infinity", []interface{}{math.Inf(1), float64(1)}, true},
	} {
		s.Run(test.name, func() {
			s.prepareFloatingClusteringTask(schemapb.DataType_Double, test.values)
			defer s.task.cleanUp(context.Background())
			s.task.compactionParams.SupportsPartialPartitionStats = false
			err := s.task.getScalarAnalyzeResult(context.Background())
			if !test.unsupported {
				s.Require().NoError(err)
				return
			}
			s.Require().ErrorIs(err, merr.ErrServiceUnimplemented)
			s.Empty(s.task.clusterBuffers, "fail before creating any output writer")
			// The upgraded coordinator builds a fresh request with its own capability;
			// original nonfinite rows can then complete without a data rewrite.
			params, err := compaction.GenerateJSONParams(s.plan.Schema)
			s.Require().NoError(err)
			s.task.compactionParams, err = compaction.ParseParamsFromJSON(params)
			s.Require().NoError(err)
			s.Require().NoError(s.task.getScalarAnalyzeResult(context.Background()))
			outputs, _, err := s.task.mapping(context.Background())
			s.Require().NoError(err)
			s.EqualValues(len(test.values), lo.SumBy(outputs, func(segment *datapb.CompactionSegment) int64 { return segment.NumOfRows }))
		})
	}
}
