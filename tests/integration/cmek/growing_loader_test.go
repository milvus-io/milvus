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

package cmek

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

// Reuse the package's native cipher fixture, but call the actual Go writer and
// loader directly. No message stream can reconstruct the prefix for this test.
func TestGrowingManifestLoader(t *testing.T) {
	t.Setenv("MILVUS_CMEK_FIXTURE_STRICT_CONTEXT", "true")
	paramtable.Init()
	params := paramtable.Get()
	cipherParams := paramtable.GetCipherParams()
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&cipherParams.SoPathGo, testGoPluginPath()},
		{&cipherParams.SoPathCpp, fixtureCppPluginPath},
	} {
		key, original := setting.item.Key, setting.item.GetValue()
		require.NoError(t, cipherParams.Save(key, setting.value))
		t.Cleanup(func() { require.NoError(t, cipherParams.Save(key, original)) })
	}
	root := t.TempDir()
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&params.CommonCfg.StorageType, "local"},
		{&params.LocalStorageCfg.Path, root},
		{&params.QueryNodeCfg.GrowingMmapEnabled, "false"},
		{&params.QueryNodeCfg.TieredEvictionEnabled, "false"},
	} {
		key, original := setting.item.Key, setting.item.GetValue()
		require.NoError(t, params.Save(key, setting.value))
		t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
	}
	initcore.InitExecExpressionFunctionFactory()
	require.NoError(t, initcore.InitLocalChunkManager(root))
	require.NoError(t, initcore.InitMmapManager(params, 1))
	require.NoError(t, initcore.InitTieredStorage(params))
	require.NoError(t, initcore.InitLocalArrowFileSystem(root))
	t.Cleanup(initcore.CleanArrowFileSystem)
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		if encrypted {
			name = "encrypted"
		}
		t.Run(name, func(t *testing.T) {
			if encrypted {
				require.NoError(t, initcore.InitPluginLoader())
				t.Cleanup(initcore.CleanPluginLoader)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			const collectionID, partitionID, segmentID, ezID = int64(71001), int64(71002), int64(71003), int64(17)
			schema := &schemapb.CollectionSchema{Name: "growing_loader", Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
				{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{
					FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			}}
			if encrypted {
				schema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}}
			}
			manager := segments.NewManager()
			require.NoError(t, manager.Collection.PutOrRef(collectionID, schema, nil,
				&querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection, CollectionID: collectionID}))
			defer manager.Collection.Unref(collectionID, 1)
			collection := manager.Collection.Get(collectionID)
			info := &querypb.SegmentLoadInfo{
				CollectionID: collectionID, PartitionID: partitionID,
				SegmentID: segmentID, Level: datapb.SegmentLevel_L1, InsertChannel: "by-dev-rootcoord-dml_0_71001v0",
			}
			source, err := segments.NewSegment(ctx, collection, manager.Segment, segments.SegmentTypeGrowing, 0, info)
			require.NoError(t, err)
			defer source.Release(ctx)
			ids := []int64{10, 20, 30}
			vectors := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
			record := &segcorepb.InsertRecord{NumRows: 3, FieldsData: []*schemapb.FieldData{
				{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}},
				}}},
				{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}},
				}}},
			}}
			require.NoError(t, source.Insert(ctx, ids, []uint64{100, 100, 100}, record))
			flushed, err := source.FlushData(ctx, 0, 3, &segments.FlushConfig{
				CollectionID: collectionID, PartitionID: partitionID, Schema: schema, WriterFormat: "parquet",
				SegmentBasePath: filepath.Join(root, name, "segment"), PartitionBasePath: filepath.Join(root, name), ReadVersion: -1,
			})
			require.NoError(t, err)
			require.NotNil(t, flushed)
			locator, err := inspector.ParseManifestLocatorV3(flushed.ManifestPath)
			require.NoError(t, err)
			manifest, err := os.ReadFile(locator.ObjectPath())
			require.NoError(t, err)
			objects, err := inspector.ParseParquetObjectsV3(manifest, locator.BasePath)
			require.NoError(t, err)
			require.NotEmpty(t, objects)
			for _, object := range objects {
				raw, err := os.ReadFile(object.Path)
				require.NoError(t, err)
				if encrypted {
					_, err = inspector.InspectEncryptedParquet(raw, ezID, collectionID)
					require.NoError(t, err)
				} else {
					require.Equal(t, "PAR1", string(raw[:4]))
				}
			}
			info = proto.Clone(info).(*querypb.SegmentLoadInfo)
			info.SegmentID++
			info.StorageVersion, info.ManifestPath, info.NumOfRows = storage.StorageV3, flushed.ManifestPath, 3
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
			loader := segments.NewLoader(ctx, manager, cm)
			loaded, err := loader.Load(ctx, collectionID, segments.SegmentTypeGrowing, 0, info)
			require.NoError(t, err)
			require.Len(t, loaded, 1)
			defer manager.Segment.Remove(ctx, info.SegmentID, querypb.DataScope_All)
			require.EqualValues(t, 3, loaded[0].RowNum())
			expr, err := proto.Marshal(&planpb.PlanNode{
				Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}, OutputFieldIds: []int64{100, 101},
			})
			require.NoError(t, err)
			plan, err := segcore.NewRetrievePlan(collection.GetCCollection(), expr, typeutil.MaxTimestamp, 0, commonpb.ConsistencyLevel_Strong, 0, 0)
			require.NoError(t, err)
			defer plan.Delete()
			result, err := loaded[0].Retrieve(ctx, plan)
			require.NoError(t, err)
			require.Equal(t, ids, result.GetIds().GetIntId().GetData())
			require.Len(t, result.GetFieldsData(), 2)
			require.Equal(t, ids, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
			require.Equal(t, vectors, result.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())
		})
	}
}
