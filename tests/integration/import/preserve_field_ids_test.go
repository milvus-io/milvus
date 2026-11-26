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

package importv2

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// The preserve-field-IDs restore suites exercise CreateCollection with
// preserve_field_ids=true followed by binlog import (StorageV2) and
// snapshot-source import (StorageV3). Dropping top-level and struct sub-fields
// leaves field-ID holes (including a trailing hole above the largest surviving
// field ID) that ordinary CreateCollection cannot reproduce; the restored
// target must preserve every surviving field, struct, sub-field, $meta,
// $namespace_id, function, and max_field_id without leaking preserve_field_ids
// into persisted collection properties.

const (
	pfiStruct = "items"
	pfiDim    = 4
)

type pfiSource struct {
	name string
}

func pfiVector(seed int64) []float32 {
	out := make([]float32, pfiDim)
	for i := range out {
		out[i] = float32(seed*10+int64(i)+1) * 0.125
	}
	return out
}

func pfiInitialSchema(name string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:               name,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "temp_drop", DataType: schemapb.DataType_Int64, Nullable: true},
			{
				Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					enKV(common.MaxLengthKey, "128"),
					enKV(common.EnableAnalyzerKey, "true"),
				},
			},
			{Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{
				Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{enKV(common.DimKey, strconv.Itoa(pfiDim))},
			},
			{Name: "tail_drop", DataType: schemapb.DataType_Int64, Nullable: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			Name:     pfiStruct,
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					Name: "code", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					Nullable:   true,
					TypeParams: []*commonpb.KeyValuePair{enKV(common.MaxCapacityKey, "8")},
				},
				{
					Name: "tag", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar,
					Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						enKV(common.MaxCapacityKey, "8"),
						enKV(common.MaxLengthKey, "32"),
					},
				},
				{
					Name: "emb", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
					Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						enKV(common.MaxCapacityKey, "8"),
						enKV(common.DimKey, strconv.Itoa(pfiDim)),
					},
				},
			},
		}},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "bm25_fn",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"sparse"},
		}},
	}
}

func (s *importTestBase) pfiDropCleanup(name string) {
	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, err := s.Cluster.MilvusClient.DropCollection(cleanupCtx, &milvuspb.DropCollectionRequest{
			CollectionName: name,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	})
}

func (s *importTestBase) pfiDescribeInternal(ctx context.Context, name string) *milvuspb.DescribeCollectionResponse {
	resp, err := s.Cluster.MixCoordClient.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection}, CollectionName: name,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	return resp
}

func (s *importTestBase) pfiCreateIndexes(ctx context.Context, name string) {
	for _, idx := range []struct {
		field     string
		indexName string
		indexType string
		metric    string
		dim       int
	}{
		{field: "vec", indexName: "idx_vec", indexType: integration.IndexFaissIvfFlat, metric: metric.L2, dim: pfiDim},
		{field: "sparse", indexName: "idx_sparse", indexType: integration.IndexSparseInvertedIndex, metric: metric.BM25, dim: 0},
		{field: typeutil.ConcatStructFieldName(pfiStruct, "emb"), indexName: "idx_items_emb", indexType: integration.IndexHNSW, metric: metric.MaxSimL2, dim: pfiDim},
	} {
		var params []*commonpb.KeyValuePair
		if idx.field == "sparse" {
			params = []*commonpb.KeyValuePair{
				enKV(common.IndexTypeKey, idx.indexType),
				enKV(common.MetricTypeKey, idx.metric),
			}
		} else {
			params = integration.ConstructIndexParam(idx.dim, idx.indexType, idx.metric)
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name,
			FieldName:      idx.field,
			IndexName:      idx.indexName,
			ExtraParams:    params,
		})
		s.Require().NoError(merr.CheckRPCCall(status, err), idx.field)
	}
}

func (s *importTestBase) pfiInsertBatch(ctx context.Context, name string, startPK int64, count int, includeDropped bool) {
	pks := make([]int64, 0, count)
	texts := make([]string, 0, count)
	vecs := make([]float32, 0, count*pfiDim)
	metaJSON := make([][]byte, 0, count)
	tempDrop := make([]int64, 0, count)
	tailDrop := make([]int64, 0, count)
	var codeScalars []*schemapb.ScalarField
	var tagScalars []*schemapb.ScalarField
	var embVectors []*schemapb.VectorField
	structValid := make([]bool, 0, count)

	for i := 0; i < count; i++ {
		pk := startPK + int64(i)
		pks = append(pks, pk)
		texts = append(texts, fmt.Sprintf("milvus vector search row %d", pk))
		vecs = append(vecs, pfiVector(pk)...)
		metaJSON = append(metaJSON, []byte(fmt.Sprintf(`{"dyn_tag":"row_%d"}`, pk)))
		tempDrop = append(tempDrop, pk*10)
		tailDrop = append(tailDrop, pk*100)

		if pk%4 == 0 {
			structValid = append(structValid, false)
			continue
		}
		structValid = append(structValid, true)
		elemCount := int(pk%3) + 1
		codes := make([]int64, 0, elemCount)
		tags := make([]string, 0, elemCount)
		embs := make([]float32, 0, elemCount*pfiDim)
		for j := 0; j < elemCount; j++ {
			codes = append(codes, pk*100+int64(j))
			tags = append(tags, fmt.Sprintf("t_%d_%d", pk, j))
			embs = append(embs, pfiVector(pk*100+int64(j))...)
		}
		codeScalars = append(codeScalars, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: codes}},
		})
		tagScalars = append(tagScalars, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: tags}},
		})
		embVectors = append(embVectors, &schemapb.VectorField{
			Dim: pfiDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: embs}},
		})
	}

	codeChild := &schemapb.FieldData{
		FieldName: "code", Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: codeScalars}},
		}},
	}
	typeutil.SetFieldDataValidData(codeChild, structValid)

	embChild := &schemapb.FieldData{
		FieldName: "emb", Type: schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: pfiDim,
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: pfiDim, ElementType: schemapb.DataType_FloatVector, Data: embVectors,
			}},
		}},
	}
	typeutil.SetFieldDataValidData(embChild, structValid)

	structChildren := []*schemapb.FieldData{codeChild}
	if includeDropped {
		tagChild := &schemapb.FieldData{
			FieldName: "tag", Type: schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: tagScalars}},
			}},
		}
		typeutil.SetFieldDataValidData(tagChild, structValid)
		structChildren = append(structChildren, tagChild)
	}
	structChildren = append(structChildren, embChild)

	fieldsData := []*schemapb.FieldData{
		{
			FieldName: "pk", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			}},
		},
		{
			FieldName: "text", Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}},
			}},
		},
		{
			FieldName: "vec", Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: pfiDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vecs}},
			}},
		},
		{
			FieldName: common.MetaFieldName, Type: schemapb.DataType_JSON, IsDynamic: true,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: metaJSON}},
			}},
		},
		{
			FieldName: pfiStruct, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
				Fields: structChildren,
			}},
		},
	}
	if includeDropped {
		validAll := make([]bool, count)
		for i := range validAll {
			validAll[i] = true
		}
		fieldsData = append(fieldsData,
			&schemapb.FieldData{
				FieldName: "temp_drop", Type: schemapb.DataType_Int64, ValidData: validAll,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: tempDrop}},
				}},
			},
			&schemapb.FieldData{
				FieldName: "tail_drop", Type: schemapb.DataType_Int64, ValidData: validAll,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: tailDrop}},
				}},
			},
		)
	}

	res, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name,
		FieldsData:     fieldsData,
		NumRows:        uint32(count),
	})
	s.Require().NoError(merr.CheckRPCCall(res.GetStatus(), err))
}

func (s *importTestBase) pfiDropField(ctx context.Context, collection, fieldName string) {
	status, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: collection,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: fieldName},
				},
			},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(status.GetAlterStatus(), err), fieldName)
}

func (s *importTestBase) pfiPrepareSource(ctx context.Context, withDelete bool) *pfiSource {
	src := &pfiSource{name: "pfi_source_" + funcutil.RandomString(8)}
	schemaBytes, err := proto.Marshal(pfiInitialSchema(src.name))
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   src.name,
		Schema:           schemaBytes,
		ShardsNum:        1,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		Properties:       []*commonpb.KeyValuePair{enKV(common.CollectionTTLConfigKey, "7200")},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.pfiDropCleanup(src.name)
	s.pfiCreateIndexes(ctx, src.name)

	s.pfiInsertBatch(ctx, src.name, 1, 8, true)
	s.enFlush(ctx, src.name)

	// Drop a middle top-level field, a middle struct sub-field, and a field
	// whose ID sits above the struct's sub-fields? Note: struct sub-fields are
	// allocated after top-level fields + $meta, so add a trailing field and drop
	// it to leave a hole strictly above the largest surviving field ID!
	addStatus, err := s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		CollectionName: src.name,
		Schema: func() []byte {
			b, mErr := proto.Marshal(&schemapb.FieldSchema{
				Name: "post_struct_drop", DataType: schemapb.DataType_Int64, Nullable: true,
			})
			s.Require().NoError(mErr)
			return b
		}(),
	})
	s.Require().NoError(merr.CheckRPCCall(addStatus, err))

	s.pfiDropField(ctx, src.name, "temp_drop")
	s.pfiDropField(ctx, src.name, typeutil.ConcatStructFieldName(pfiStruct, "tag"))
	s.pfiDropField(ctx, src.name, "tail_drop")
	s.pfiDropField(ctx, src.name, "post_struct_drop")

	if withDelete {
		delResp, delErr := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
			CollectionName: src.name, Expr: "pk in [2, 6]",
		})
		s.Require().NoError(merr.CheckRPCCall(delResp.GetStatus(), delErr))
	}

	s.pfiInsertBatch(ctx, src.name, 101, 8, false)
	s.enFlush(ctx, src.name)
	return src
}

func (s *importTestBase) pfiCreatePreservedTarget(ctx context.Context, sourceName string) string {
	described, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: sourceName,
	})
	s.Require().NoError(merr.CheckRPCCall(described.GetStatus(), err))

	target := "pfi_target_" + funcutil.RandomString(8)
	schema := proto.Clone(described.GetSchema()).(*schemapb.CollectionSchema)
	schema.Name = target

	internalDesc := s.pfiDescribeInternal(ctx, sourceName)
	for _, field := range internalDesc.GetSchema().GetFields() {
		if field.GetIsDynamic() && field.GetName() == common.MetaFieldName {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
				FieldID: field.GetFieldID(), Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true,
			})
		}
		if field.GetName() == common.NamespaceFieldName {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
				FieldID: field.GetFieldID(), Name: common.NamespaceFieldName, DataType: schemapb.DataType_VarChar,
			})
		}
	}

	properties := append([]*commonpb.KeyValuePair{}, described.GetProperties()...)
	properties = append(properties, enKV(util.PreserveFieldIdsKey, "true"))
	schemaBytes, err := proto.Marshal(schema)
	s.Require().NoError(err)

	var numPartitions int64
	if schema.GetEnableNamespace() && common.IsNamespaceModePartitionKey(properties...) {
		numPartitions = described.GetNumPartitions()
	}
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   target,
		Schema:           schemaBytes,
		ShardsNum:        described.GetShardsNum(),
		NumPartitions:    numPartitions,
		Properties:       properties,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.pfiDropCleanup(target)
	s.pfiRequireSameMetadataAndCleanProperties(ctx, sourceName, target)
	return target
}

func (s *importTestBase) pfiRequireSameMetadataAndCleanProperties(ctx context.Context, source, target string) {
	collectIDs := func(schema *schemapb.CollectionSchema) map[string]int64 {
		out := make(map[string]int64)
		for _, field := range schema.GetFields() {
			out[field.GetName()] = field.GetFieldID()
		}
		for _, structField := range schema.GetStructArrayFields() {
			out[structField.GetName()] = structField.GetFieldID()
			for _, subField := range structField.GetFields() {
				out[subField.GetName()] = subField.GetFieldID()
			}
		}
		return out
	}

	sourceInternal := s.pfiDescribeInternal(ctx, source)
	targetInternal := s.pfiDescribeInternal(ctx, target)
	s.Require().Equal(collectIDs(sourceInternal.GetSchema()), collectIDs(targetInternal.GetSchema()))

	s.Require().Len(targetInternal.GetSchema().GetFunctions(), len(sourceInternal.GetSchema().GetFunctions()))
	for i, srcFn := range sourceInternal.GetSchema().GetFunctions() {
		dstFn := targetInternal.GetSchema().GetFunctions()[i]
		s.Require().Equal(srcFn.GetId(), dstFn.GetId())
		s.Require().Equal(srcFn.GetInputFieldIds(), dstFn.GetInputFieldIds())
		s.Require().Equal(srcFn.GetOutputFieldIds(), dstFn.GetOutputFieldIds())
	}

	sourcePublic, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: source})
	s.Require().NoError(merr.CheckRPCCall(sourcePublic.GetStatus(), err))
	targetPublic, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: target})
	s.Require().NoError(merr.CheckRPCCall(targetPublic.GetStatus(), err))

	srcMax, srcFound := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, sourcePublic.GetProperties())
	dstMax, dstFound := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, targetPublic.GetProperties())
	s.Require().True(srcFound)
	s.Require().True(dstFound)
	s.Require().Equal(srcMax, dstMax, "max_field_id must preserve trailing dropped field holes")

	// preserve_field_ids must never leak into persisted collection or schema properties.
	_, leakedPublic := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, targetPublic.GetProperties())
	s.Require().False(leakedPublic, "preserve_field_ids must not appear in DescribeCollection.Properties")
	_, leakedSchema := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, targetPublic.GetSchema().GetProperties())
	s.Require().False(leakedSchema, "preserve_field_ids must not appear in DescribeCollection.Schema.Properties")
}

func (s *importTestBase) pfiLoadAndCompare(ctx context.Context, source, target string) {
	for _, col := range []string{source, target} {
		s.WaitForIndexBuilt(ctx, col, "vec")
		s.WaitForIndexBuilt(ctx, col, "sparse")
		s.WaitForIndexBuilt(ctx, col, typeutil.ConcatStructFieldName(pfiStruct, "emb"))
		status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: col})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForLoad(ctx, col)
	}

	queryRows := func(col string) map[int64]string {
		resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName:   col,
			Expr:             "pk >= 0",
			OutputFields:     []string{"pk", "text", common.MetaFieldName, pfiStruct},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
		var pks []int64
		var texts []string
		var dyn [][]byte
		var structs *schemapb.FieldData
		for _, fd := range resp.GetFieldsData() {
			switch fd.GetFieldName() {
			case "pk":
				pks = fd.GetScalars().GetLongData().GetData()
			case "text":
				texts = fd.GetScalars().GetStringData().GetData()
			case common.MetaFieldName, "dyn_tag":
				dyn = fd.GetScalars().GetJsonData().GetData()
			case pfiStruct:
				structs = fd
			}
		}
		s.Require().NotEmpty(pks)
		s.Require().NotNil(structs)
		rows := make(map[int64]string, len(pks))
		for i, pk := range pks {
			var codeVals []int64
			isNull := false
			for _, child := range structs.GetStructArrays().GetFields() {
				subName, nameErr := typeutil.ExtractStructFieldName(child.GetFieldName())
				s.Require().NoError(nameErr)
				if valid := child.GetValidData(); len(valid) > 0 && !valid[i] {
					isNull = true
					continue
				}
				if subName == "code" {
					codeVals = child.GetScalars().GetArrayData().GetData()[i].GetLongData().GetData()
				}
			}
			rows[pk] = fmt.Sprintf("text=%s|dyn=%s|null=%v|codes=%v", texts[i], string(dyn[i]), isNull, codeVals)
		}
		return rows
	}

	srcRows := queryRows(source)
	dstRows := queryRows(target)
	s.Require().Equal(srcRows, dstRows)
}

type PreserveFieldIDsBinlogImportV2Suite struct {
	importTestBase
}

func (s *PreserveFieldIDsBinlogImportV2Suite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportScheduleInterval.Key, "0.1")
	s.importTestBase.SetupSuite()
}

func (s *PreserveFieldIDsBinlogImportV2Suite) TestBinlogImportWithPreservedFieldIDs() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()

	src := s.pfiPrepareSource(ctx, false)
	target := s.pfiCreatePreservedTarget(ctx, src.name)
	s.pfiCreateIndexes(ctx, target)

	segments, err := s.Cluster.ShowSegments(src.name)
	s.Require().NoError(err)
	var files []*internalpb.ImportFile
	for _, seg := range segments {
		if seg.GetState() != commonpb.SegmentState_Flushed || seg.GetLevel() != datapb.SegmentLevel_L1 {
			continue
		}
		s.Require().EqualValues(2, seg.GetStorageVersion())
		files = append(files, &internalpb.ImportFile{Paths: []string{path.Join(
			s.Cluster.RootPath(), common.SegmentInsertLogPath,
			strconv.FormatInt(seg.GetCollectionID(), 10),
			strconv.FormatInt(seg.GetPartitionID(), 10),
			strconv.FormatInt(seg.GetID(), 10),
		)}})
	}
	s.Require().NotEmpty(files)

	impResp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: target,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			enKV(importutilv2.BackupFlag, "true"),
			enKV(importutilv2.StorageVersion, "2"),
		},
	})
	s.Require().NoError(merr.CheckRPCCall(impResp.GetStatus(), err))
	s.Require().NoError(WaitForImportDone(ctx, s.Cluster, impResp.GetJobID()))

	s.pfiLoadAndCompare(ctx, src.name, target)

	// Verify that adding a field to target allocates strictly above preserved max_field_id.
	beforeDesc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: target})
	s.Require().NoError(merr.CheckRPCCall(beforeDesc.GetStatus(), err))
	maxStr, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, beforeDesc.GetProperties())
	s.Require().True(ok)
	expectedNextID, err := strconv.ParseInt(maxStr, 10, 64)
	s.Require().NoError(err)
	expectedNextID++

	newFieldBytes, err := proto.Marshal(&schemapb.FieldSchema{
		Name: "after_restore", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	s.Require().NoError(err)
	addStatus, err := s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		CollectionName: target, Schema: newFieldBytes,
	})
	s.Require().NoError(merr.CheckRPCCall(addStatus, err))

	afterDesc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: target})
	s.Require().NoError(merr.CheckRPCCall(afterDesc.GetStatus(), err))
	foundNewField := false
	for _, f := range afterDesc.GetSchema().GetFields() {
		if f.GetName() == "after_restore" {
			foundNewField = true
			s.Require().Equal(expectedNextID, f.GetFieldID())
		}
	}
	s.Require().True(foundNewField)
}

func TestPreserveFieldIDsBinlogImportV2(t *testing.T) {
	suite.Run(t, new(PreserveFieldIDsBinlogImportV2Suite))
}

type PreserveFieldIDsSnapshotImportV3Suite struct {
	importTestBase
}

func (s *PreserveFieldIDsSnapshotImportV3Suite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportScheduleInterval.Key, "0.1")
	s.importTestBase.SetupSuite()
}

func (s *PreserveFieldIDsSnapshotImportV3Suite) TestSnapshotSourceImportWithPreservedFieldIDs() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()

	src := s.pfiPrepareSource(ctx, true)
	snapName := "pfi_snap_" + funcutil.RandomString(8)
	snapStatus, err := s.Cluster.MilvusClient.CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{
		Name: snapName, CollectionName: src.name,
	})
	s.Require().NoError(merr.CheckRPCCall(snapStatus, err))
	s.T().Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		dropStatus, dropErr := s.Cluster.MilvusClient.DropSnapshot(cleanupCtx, &milvuspb.DropSnapshotRequest{
			Name: snapName, CollectionName: src.name,
		})
		s.NoError(merr.CheckRPCCall(dropStatus, dropErr))
	})
	snapDesc, err := s.Cluster.MilvusClient.DescribeSnapshot(ctx, &milvuspb.DescribeSnapshotRequest{
		Name: snapName, CollectionName: src.name,
	})
	s.Require().NoError(merr.CheckRPCCall(snapDesc.GetStatus(), err))

	target := s.pfiCreatePreservedTarget(ctx, src.name)
	s.pfiCreateIndexes(ctx, target)

	impResp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: target,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          []*internalpb.ImportFile{{Paths: []string{snapDesc.GetS3Location()}}},
		Options: []*commonpb.KeyValuePair{
			enKV(importutilv2.BackupFlag, "true"),
			enKV(importutilv2.SourceType, importutilv2.SourceTypeSnapshot),
		},
	})
	s.Require().NoError(merr.CheckRPCCall(impResp.GetStatus(), err))
	s.Require().NoError(WaitForImportDone(ctx, s.Cluster, impResp.GetJobID()))

	s.pfiLoadAndCompare(ctx, src.name, target)
}

func (s *PreserveFieldIDsSnapshotImportV3Suite) TestNamespaceCollectionPreservesFieldIDs() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 10*time.Minute)
	defer cancel()

	srcName := "pfi_ns_src_" + funcutil.RandomString(8)
	srcSchema := &schemapb.CollectionSchema{
		Name:               srcName,
		EnableDynamicField: true,
		EnableNamespace:    true,
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "hole", DataType: schemapb.DataType_Int64, Nullable: true},
			{
				Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{enKV(common.DimKey, strconv.Itoa(pfiDim))},
			},
		},
	}
	schemaBytes, err := proto.Marshal(srcSchema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   srcName,
		Schema:           schemaBytes,
		ShardsNum:        1,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.pfiDropCleanup(srcName)

	s.pfiDropField(ctx, srcName, "hole")

	target := s.pfiCreatePreservedTarget(ctx, srcName)

	// Verify partitionkey.isolation is present exactly once and namespace write/read works.
	targetDesc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: target})
	s.Require().NoError(merr.CheckRPCCall(targetDesc.GetStatus(), err))
	isolationCount := 0
	for _, kv := range targetDesc.GetProperties() {
		if kv.GetKey() == common.PartitionKeyIsolationKey {
			isolationCount++
		}
	}
	s.Require().Equal(1, isolationCount)

	// Verify an explicit multi-partition count is preserved when restoring a
	// namespace collection with a minimal $namespace_id stub (IsPartitionKey omitted).
	multiTarget := "pfi_ns_multi_" + funcutil.RandomString(8)
	multiSchema := proto.Clone(targetDesc.GetSchema()).(*schemapb.CollectionSchema)
	multiSchema.Name = multiTarget
	for _, field := range s.pfiDescribeInternal(ctx, srcName).GetSchema().GetFields() {
		switch field.GetName() {
		case common.MetaFieldName:
			multiSchema.Fields = append(multiSchema.Fields, &schemapb.FieldSchema{
				FieldID: field.GetFieldID(), Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true,
			})
		case common.NamespaceFieldName:
			multiSchema.Fields = append(multiSchema.Fields, &schemapb.FieldSchema{
				FieldID: field.GetFieldID(), Name: common.NamespaceFieldName, DataType: schemapb.DataType_VarChar,
			})
		}
	}
	multiSchemaBytes, err := proto.Marshal(multiSchema)
	s.Require().NoError(err)
	multiProps := append(append([]*commonpb.KeyValuePair{}, targetDesc.GetProperties()...), enKV(util.PreserveFieldIdsKey, "true"))
	multiStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   multiTarget,
		Schema:           multiSchemaBytes,
		ShardsNum:        1,
		NumPartitions:    8,
		Properties:       multiProps,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(multiStatus, err))
	s.pfiDropCleanup(multiTarget)
	multiDesc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: multiTarget})
	s.Require().NoError(merr.CheckRPCCall(multiDesc.GetStatus(), err))
	s.Require().Equal(int64(8), multiDesc.GetNumPartitions())

	idxStatus, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: target,
		FieldName:      "vec",
		IndexName:      "idx_vec",
		ExtraParams:    integration.ConstructIndexParam(pfiDim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(idxStatus, err))
	s.WaitForIndexBuilt(ctx, target, "vec")
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: target})
	s.Require().NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, target)

	ns := "tenant_a"
	insResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: target,
		Namespace:      &ns,
		NumRows:        1,
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "pk", Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42}}},
				}},
			},
			{
				FieldName: "vec", Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: pfiDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: pfiVector(42)}},
				}},
			},
			{
				FieldName: common.MetaFieldName, Type: schemapb.DataType_JSON, IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"k":"v"}`)}}},
				}},
			},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(insResp.GetStatus(), err))

	qResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   target,
		Namespace:        &ns,
		Expr:             "pk == 42",
		OutputFields:     []string{"pk", "k"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(qResp.GetStatus(), err))
	s.Require().NotEmpty(qResp.GetFieldsData())
}

func TestPreserveFieldIDsSnapshotImportV3(t *testing.T) {
	suite.Run(t, new(PreserveFieldIDsSnapshotImportV3Suite))
}
