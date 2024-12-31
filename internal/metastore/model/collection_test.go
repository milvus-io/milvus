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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/common"
)

var (
	colID        int64 = 1
	colName            = "c"
	fieldID      int64 = 101
	fieldName          = "field110"
	partID       int64 = 20
	partName           = "testPart"
	tenantID           = "tenant-1"
	functionID   int64 = 1
	functionName       = "test-bm25"
	typeParams         = []*commonpb.KeyValuePair{
		{
			Key:   "field110-k1",
			Value: "field110-v1",
		},
	}
	startPositions = []*commonpb.KeyDataPair{
		{
			Key:  "k1",
			Data: []byte{byte(1)},
		},
	}

	colModel = &Collection{
		TenantID:             tenantID,
		CollectionID:         colID,
		Name:                 colName,
		AutoID:               false,
		Description:          "none",
		Fields:               []*Field{fieldModel},
		VirtualChannelNames:  []string{"vch"},
		PhysicalChannelNames: []string{"pch"},
		ShardsNum:            common.DefaultShardsNum,
		CreateTime:           1,
		StartPositions:       startPositions,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Strong,
		Partitions: []*Partition{
			{
				PartitionID:               partID,
				PartitionName:             partName,
				PartitionCreatedTimestamp: 1,
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   "k",
				Value: "v",
			},
		},
	}

	deprecatedColPb = &pb.CollectionInfo{
		ID: colID,
		Schema: &schemapb.CollectionSchema{
			Name:        colName,
			Description: "none",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{filedSchemaPb},
		},
		CreateTime:                 1,
		PartitionIDs:               []int64{partID},
		PartitionNames:             []string{partName},
		PartitionCreatedTimestamps: []uint64{1},
		FieldIndexes: []*pb.FieldIndexInfo{
			{
				FiledID: fieldID,
				IndexID: indexID,
			},
		},
		VirtualChannelNames:  []string{"vch"},
		PhysicalChannelNames: []string{"pch"},
		ShardsNum:            common.DefaultShardsNum,
		StartPositions:       startPositions,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Strong,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   "k",
				Value: "v",
			},
		},
	}
)

func TestUnmarshalCollectionModel(t *testing.T) {
	ret := UnmarshalCollectionModel(deprecatedColPb)
	ret.TenantID = tenantID
	assert.Equal(t, ret, colModel)

	assert.Nil(t, UnmarshalCollectionModel(nil))
}

func TestMarshalCollectionModel(t *testing.T) {
	assert.Nil(t, MarshalCollectionModel(nil))
}

func TestCollection_GetPartitionNum(t *testing.T) {
	coll := &Collection{
		Partitions: []*Partition{
			{State: pb.PartitionState_PartitionCreated},
			{State: pb.PartitionState_PartitionCreating},
			{State: pb.PartitionState_PartitionCreated},
			{State: pb.PartitionState_PartitionDropping},
			{State: pb.PartitionState_PartitionCreated},
			{State: pb.PartitionState_PartitionDropped},
		},
	}
	assert.Equal(t, 3, coll.GetPartitionNum(true))
	assert.Equal(t, 6, coll.GetPartitionNum(false))
}

func TestCollection_Equal(t *testing.T) {
	equal := func(a, b Collection) bool {
		return a.Equal(b)
	}

	type args struct {
		a, b Collection
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				a: Collection{TenantID: "aaa"},
				b: Collection{TenantID: "bbb"},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:   "aaa",
					Partitions: []*Partition{{PartitionName: "default"}},
				},
				b: Collection{
					TenantID: "aaa",
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:   "aaa",
					Partitions: []*Partition{{PartitionName: "default"}},
					Name:       "aaa",
				},
				b: Collection{
					TenantID:   "aaa",
					Partitions: []*Partition{{PartitionName: "default"}},
					Name:       "bbb",
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "bbb",
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      true,
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "256"},
					}}},
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum: 1,
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum: 2,
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					Properties: []*commonpb.KeyValuePair{
						{Key: "ttl", Value: "200"},
					},
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					Properties: []*commonpb.KeyValuePair{
						{Key: "ttl", Value: "100"},
					},
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					Properties: []*commonpb.KeyValuePair{
						{Key: "ttl", Value: "200"},
					},
					EnableDynamicField: false,
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					Properties: []*commonpb.KeyValuePair{
						{Key: "ttl", Value: "200"},
					},
					EnableDynamicField: true,
				},
			},
			want: false,
		},
		{
			args: args{
				a: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					Properties: []*commonpb.KeyValuePair{
						{Key: "ttl", Value: "200"},
					},
					EnableDynamicField: false,
				},
				b: Collection{
					TenantID:    "aaa",
					Partitions:  []*Partition{{PartitionName: "default"}},
					Name:        "aaa",
					Description: "aaa",
					AutoID:      false,
					Fields: []*Field{{Name: "f1", TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}}},
					ShardsNum:        1,
					ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					Properties: []*commonpb.KeyValuePair{
						{Key: "ttl", Value: "200"},
					},
					EnableDynamicField: false,
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := equal(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("equal() = %v, want %v, collection a: %v, collection b: %v", got, tt.want, tt.args.a, tt.args.b)
			}
		})
	}
}

func TestClone(t *testing.T) {
	collection := &Collection{
		TenantID:     "1",
		DBID:         2,
		CollectionID: 3,
		Partitions: []*Partition{
			{
				PartitionID:               4,
				PartitionName:             "5",
				PartitionCreatedTimestamp: 6,
				CollectionID:              7,
				State:                     pb.PartitionState_PartitionCreated,
			},
			{
				PartitionID:               8,
				PartitionName:             "9",
				PartitionCreatedTimestamp: 10,
				CollectionID:              11,
				State:                     pb.PartitionState_PartitionCreating,
			},
		},
		Name:        "12",
		DBName:      "13",
		Description: "14",
		AutoID:      true,
		Fields: []*Field{
			{
				FieldID:          15,
				Name:             "16",
				IsPrimaryKey:     false,
				Description:      "17",
				DataType:         schemapb.DataType_Double,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "18", Value: "19"}},
				IndexParams:      []*commonpb.KeyValuePair{{Key: "20", Value: "21"}},
				AutoID:           true,
				State:            schemapb.FieldState_FieldDropping,
				IsDynamic:        true,
				IsPartitionKey:   false,
				IsClusteringKey:  true,
				IsFunctionOutput: false,
				DefaultValue:     nil,
				ElementType:      schemapb.DataType_String,
				Nullable:         true,
			},
			{
				FieldID:          22,
				Name:             "23",
				IsPrimaryKey:     true,
				Description:      "24",
				DataType:         schemapb.DataType_FloatVector,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "25", Value: "26"}},
				IndexParams:      []*commonpb.KeyValuePair{{Key: "27", Value: "28"}},
				AutoID:           true,
				State:            schemapb.FieldState_FieldCreating,
				IsDynamic:        true,
				IsPartitionKey:   false,
				IsClusteringKey:  true,
				IsFunctionOutput: false,
				DefaultValue:     nil,
				ElementType:      schemapb.DataType_VarChar,
				Nullable:         true,
			},
		},
		Functions: []*Function{
			{
				ID:               functionID,
				Name:             functionName,
				Type:             schemapb.FunctionType_BM25,
				InputFieldIDs:    []int64{101},
				InputFieldNames:  []string{"text"},
				OutputFieldIDs:   []int64{103},
				OutputFieldNames: []string{"sparse"},
			},
		},
		VirtualChannelNames:  []string{"c1", "c2"},
		PhysicalChannelNames: []string{"c3", "c4"},
		ShardsNum:            2,
		StartPositions:       startPositions,
		CreateTime:           1234,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Eventually,
		Aliases:              []string{"a1", "a2"},
		Properties:           []*commonpb.KeyValuePair{{Key: "32", Value: "33"}},
		State:                pb.CollectionState_CollectionCreated,
		EnableDynamicField:   true,
	}

	clone1 := collection.Clone()
	assert.Equal(t, clone1, collection)
	clone2 := collection.ShadowClone()
	assert.Equal(t, clone2, collection)
}
