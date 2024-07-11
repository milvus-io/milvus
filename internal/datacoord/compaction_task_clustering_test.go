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

package datacoord

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
)

func (s *CompactionTaskSuite) TestClusteringCompactionSegmentMetaChange() {
	channel := "Ch-1"
	cm := storage.NewLocalChunkManager(storage.RootPath(""))
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	meta, err := newMeta(context.TODO(), catalog, cm)
	s.NoError(err)
	meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:    101,
			State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L1,
		},
	})
	meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    102,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			PartitionStatsVersion: 10000,
		},
	})
	session := NewSessionManagerImpl()

	schema := ConstructScalarClusteringSchema("TestClusteringCompactionTask", 32, true)
	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            Int64Field,
		IsPrimaryKey:    true,
		Description:     "",
		DataType:        schemapb.DataType_Int64,
		TypeParams:      nil,
		IndexParams:     nil,
		AutoID:          true,
		IsClusteringKey: true,
	}

	task := &clusteringCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:             1,
			TriggerID:          19530,
			CollectionID:       1,
			PartitionID:        10,
			Channel:            channel,
			Type:               datapb.CompactionType_ClusteringCompaction,
			NodeID:             1,
			State:              datapb.CompactionTaskState_pipelining,
			Schema:             schema,
			ClusteringKeyField: pk,
			InputSegments:      []int64{101, 102},
		},
		meta:     meta,
		sessions: session,
	}

	task.processPipelining()

	seg11 := meta.GetSegment(101)
	s.Equal(datapb.SegmentLevel_L2, seg11.Level)
	seg21 := meta.GetSegment(102)
	s.Equal(datapb.SegmentLevel_L2, seg21.Level)
	s.Equal(int64(10000), seg21.PartitionStatsVersion)

	task.ResultSegments = []int64{103, 104}
	// fake some compaction result segment
	meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    103,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			CreatedByCompaction:   true,
			PartitionStatsVersion: 10001,
		},
	})
	meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    104,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			CreatedByCompaction:   true,
			PartitionStatsVersion: 10001,
		},
	})

	task.processFailedOrTimeout()

	seg12 := meta.GetSegment(101)
	s.Equal(datapb.SegmentLevel_L1, seg12.Level)
	seg22 := meta.GetSegment(102)
	s.Equal(datapb.SegmentLevel_L2, seg22.Level)
	s.Equal(int64(10000), seg22.PartitionStatsVersion)

	seg32 := meta.GetSegment(103)
	s.Equal(datapb.SegmentLevel_L1, seg32.Level)
	s.Equal(int64(0), seg32.PartitionStatsVersion)
	seg42 := meta.GetSegment(104)
	s.Equal(datapb.SegmentLevel_L1, seg42.Level)
	s.Equal(int64(0), seg42.PartitionStatsVersion)
}

const (
	Int64Field    = "int64Field"
	FloatVecField = "floatVecField"
)

func ConstructScalarClusteringSchema(collection string, dim int, autoID bool, fields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	// if fields are specified, construct it
	if len(fields) > 0 {
		return &schemapb.CollectionSchema{
			Name:   collection,
			AutoID: autoID,
			Fields: fields,
		}
	}

	// if no field is specified, use default
	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            Int64Field,
		IsPrimaryKey:    true,
		Description:     "",
		DataType:        schemapb.DataType_Int64,
		TypeParams:      nil,
		IndexParams:     nil,
		AutoID:          autoID,
		IsClusteringKey: true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	return &schemapb.CollectionSchema{
		Name:   collection,
		AutoID: autoID,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}
}
