package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	pb "github.com/milvus-io/milvus/pkg/proto/etcdpb"
)

var (
	colID      int64 = 1
	colName          = "c"
	fieldID    int64 = 101
	fieldName        = "field110"
	partID     int64 = 20
	partName         = "testPart"
	tenantID         = "tenant-1"
	typeParams       = []*commonpb.KeyValuePair{
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
