package rootcoord

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
)

const memBenchCollections = 1000

// memBenchView holds what RootCoord's collection view keeps per
// collection beyond the model: the schema and the properties.
type memBenchView struct {
	Schema        *schemapb.CollectionSchema
	Properties    map[string]string
	PropertyPairs []*commonpb.KeyValuePair
}

// buildMemBenchView builds the view the way
// MetaTable.buildCollectionViewLocked does.
func buildMemBenchView(coll *model.Collection) *memBenchView {
	schema := coll.SchemaView()
	schema.DbName = util.DefaultDBName
	return &memBenchView{
		Schema:        schema,
		PropertyPairs: coll.Properties,
	}
}

func memBenchKV(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func memBenchField(id int64, name string, dt schemapb.DataType, params ...*commonpb.KeyValuePair) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:     id,
		Name:        name,
		Description: name + " field",
		DataType:    dt,
		TypeParams:  params,
	}
}

// memBenchCollectionInfo returns collection i: 16 fields (pk, varchar,
// scalars, two vectors), a struct array field with two sub-fields, one
// partition, one shard and a few properties.
func memBenchCollectionInfo(i int) *pb.CollectionInfo {
	collID := int64(100000 + i)
	fields := []*schemapb.FieldSchema{
		memBenchField(common.RowIDField, common.RowIDFieldName, schemapb.DataType_Int64),
		memBenchField(common.TimeStampField, common.TimeStampFieldName, schemapb.DataType_Int64),
		{FieldID: 100, Name: "id", Description: "primary key", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		memBenchField(101, "title", schemapb.DataType_VarChar, memBenchKV(common.MaxLengthKey, "512")),
		memBenchField(102, "category", schemapb.DataType_VarChar,
			memBenchKV(common.MaxLengthKey, "64"), memBenchKV("enable_match", "true")),
		memBenchField(103, "price", schemapb.DataType_Double),
		memBenchField(104, "count", schemapb.DataType_Int32),
		memBenchField(105, "flag", schemapb.DataType_Bool),
		memBenchField(106, "score", schemapb.DataType_Float, memBenchKV(common.MmapEnabledKey, "true")),
		memBenchField(107, "created", schemapb.DataType_Int64),
		{
			FieldID: 108, Name: "tags", Description: "tags field", DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams:  []*commonpb.KeyValuePair{memBenchKV(common.MaxLengthKey, "32"), memBenchKV(common.MaxCapacityKey, "16")},
		},
		memBenchField(109, "meta", schemapb.DataType_JSON),
		memBenchField(110, "region", schemapb.DataType_VarChar, memBenchKV(common.MaxLengthKey, "128")),
		memBenchField(111, "rank", schemapb.DataType_Int16),
		memBenchField(112, "embedding", schemapb.DataType_FloatVector, memBenchKV(common.DimKey, "768")),
		memBenchField(113, "sparse_or_img", schemapb.DataType_FloatVector, memBenchKV(common.DimKey, "128")),
	}
	structField := &schemapb.StructArrayFieldSchema{
		FieldID:     114,
		Name:        "chunks",
		Description: "chunks field",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID: 115, Name: "chunk_text", Description: "chunk text", DataType: schemapb.DataType_Array,
				ElementType: schemapb.DataType_VarChar,
				TypeParams:  []*commonpb.KeyValuePair{memBenchKV(common.MaxLengthKey, "1024"), memBenchKV(common.MaxCapacityKey, "64")},
			},
			{
				FieldID: 116, Name: "chunk_vec", Description: "chunk vector", DataType: schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams:  []*commonpb.KeyValuePair{memBenchKV(common.DimKey, "384"), memBenchKV(common.MaxCapacityKey, "64")},
			},
		},
	}
	return &pb.CollectionInfo{
		ID:   collID,
		DbId: util.DefaultDBID,
		Schema: &schemapb.CollectionSchema{
			Name:               fmt.Sprintf("bench_collection_%d", i),
			Description:        "memory benchmark collection",
			Fields:             fields,
			StructArrayFields:  []*schemapb.StructArrayFieldSchema{structField},
			EnableDynamicField: true,
			Version:            1,
		},
		PartitionIDs:               []int64{collID + 1},
		PartitionNames:             []string{"_default"},
		PartitionCreatedTimestamps: []uint64{1},
		VirtualChannelNames:        []string{fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collID)},
		PhysicalChannelNames:       []string{"by-dev-rootcoord-dml_0"},
		ShardsNum:                  1,
		ConsistencyLevel:           commonpb.ConsistencyLevel_Bounded,
		CreateTime:                 1,
		UpdateTimestamp:            1,
		State:                      pb.CollectionState_CollectionCreated,
		Properties: []*commonpb.KeyValuePair{
			memBenchKV(common.CollectionTTLConfigKey, "86400"),
			memBenchKV(common.MmapEnabledKey, "true"),
			memBenchKV(common.CollectionAutoCompactionKey, "true"),
			memBenchKV("collection.replica.number", strconv.Itoa(1)),
		},
	}
}

func memBenchHeapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// BenchmarkCatalogCollectionMemory measures the heap RootCoord retains per
// collection after a startup load: the models ListCollections returns
// ("model"), and those models plus the collection views built from them
// ("model+view"). The KV store itself is in the baseline.
func BenchmarkCatalogCollectionMemory(b *testing.B) {
	ctx := context.Background()
	kc := NewCatalog(memkv.NewMemoryKV())
	for i := 0; i < memBenchCollections; i++ {
		coll := model.UnmarshalCollectionModel(memBenchCollectionInfo(i))
		if err := kc.CreateCollection(ctx, coll, 0); err != nil {
			b.Fatal(err)
		}
	}

	var modelBytes, viewBytes float64
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		before := memBenchHeapAlloc()
		b.StartTimer()

		colls, err := kc.ListCollections(ctx, util.DefaultDBID, 0)
		if err != nil {
			b.Fatal(err)
		}
		if len(colls) != memBenchCollections {
			b.Fatalf("loaded %d collections, want %d", len(colls), memBenchCollections)
		}

		b.StopTimer()
		afterModel := memBenchHeapAlloc()
		b.StartTimer()

		views := make([]*memBenchView, len(colls))
		for i, coll := range colls {
			views[i] = buildMemBenchView(coll)
		}

		b.StopTimer()
		afterView := memBenchHeapAlloc()
		runtime.KeepAlive(colls)
		runtime.KeepAlive(views)
		modelBytes += float64(afterModel) - float64(before)
		viewBytes += float64(afterView) - float64(before)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(modelBytes/float64(b.N)/memBenchCollections, "model-retained-B/coll")
	b.ReportMetric(viewBytes/float64(b.N)/memBenchCollections, "model+view-retained-B/coll")
}
