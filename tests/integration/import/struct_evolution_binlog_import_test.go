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
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// The struct evolution binlog import suites import segments of a collection
// whose struct evolved after data was written, through the two import-based
// restore paths:
//   - binlog import of StorageV2 segments (TestStructEvolutionBinlogImportV2);
//   - snapshot-source import of StorageV3 segments (TestStructEvolutionSnapshotSourceImportV3).
//
// The source struct holds a legacy Int64 sub-field, an element-nullable
// VarChar, a nested Array<Array<Int64>>, an element-nullable and a legacy
// FloatVector. After the first batch it gains an element-nullable Int64 and an
// element-nullable FloatVector sub-field, and later loses the VarChar one. The
// target collection is created by replaying the same initial schema and schema
// evolution so its field IDs match the source without relying on
// preserve_field_ids. The imported target must read back every row like the
// source and like the model the rows were built from.

// sebiAddedVector is the element-nullable vector sub-field added to the struct.
// Rows never carry it, so it reads back as one null per element.
const sebiAddedVector = "av"

// sebiChildren are the sub-fields after the evolution, without sebiAddedVector.
var sebiChildren = []string{"id", "tags", "nvec", "vec", "c"}

func sebiAddedVectorSchema() *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name:     typeutil.ConcatStructFieldName(enStruct, sebiAddedVector),
		DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
		Nullable: true, ElementNullable: true,
		TypeParams: []*commonpb.KeyValuePair{enKV(common.MaxCapacityKey, "10"), enKV(common.DimKey, strconv.Itoa(enDim))},
	}
}

type sebiSource struct {
	name    string
	model   []enRow
	deleted map[int64]bool
}

// expected is the query output of the source after the evolution.
func (src *sebiSource) expected() map[int64]map[string]any {
	rows := make([]enRow, 0, len(src.model))
	for _, row := range src.model {
		if !src.deleted[row.pk] {
			rows = append(rows, row)
		}
	}
	out := enExpectedRows(rows, sebiChildren)
	for _, row := range rows {
		if row.null {
			out[row.pk][sebiAddedVector] = nil
			continue
		}
		out[row.pk][sebiAddedVector] = make([]any, len(row.elems))
	}
	return out
}

func (s *importTestBase) sebiCreateAddedVectorIndex(ctx context.Context, name string) {
	field := typeutil.ConcatStructFieldName(enStruct, sebiAddedVector)
	status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: field, IndexName: "idx_added_vector",
		ExtraParams: integration.ConstructIndexParam(enDim, integration.IndexHNSW, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err), field)
}

func (s *importTestBase) sebiLoad(ctx context.Context, name string) {
	s.WaitForIndexBuilt(ctx, name, typeutil.ConcatStructFieldName(enStruct, sebiAddedVector))
	s.enLoad(ctx, name)
}

func (s *importTestBase) sebiDropCleanup(name string) {
	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, err := s.Cluster.MilvusClient.DropCollection(cleanupCtx, &milvuspb.DropCollectionRequest{
			CollectionName: name,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	})
}

func (s *importTestBase) sebiEvolveSchema(ctx context.Context, name string) {
	s.enAddSubField(ctx, name, "c")
	s.enAlter(ctx, name, &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
		AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
			FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{FieldSchema: sebiAddedVectorSchema()}},
		},
	}})
	s.sebiCreateAddedVectorIndex(ctx, name)
}

// sebiPrepareSource creates the source collection, writes three batches around
// the struct evolution and, when withDelete is set, deletes rows written
// before the last batch.
func (s *importTestBase) sebiPrepareSource(ctx context.Context, withDelete bool) *sebiSource {
	src := &sebiSource{name: "sebi_source_" + funcutil.RandomString(8), deleted: map[int64]bool{}}
	schema := enSchema(src.name, enBaseChildren)
	schema.EnableDynamicField = true
	s.enCreate(ctx, schema)
	s.sebiDropCleanup(src.name)
	s.enCreateIndexes(ctx, src.name)

	batch := enBatch(1, 24, false)
	s.enInsert(ctx, src.name, batch, enBaseChildren)
	s.enFlush(ctx, src.name)
	src.model = append(src.model, batch...)

	s.sebiEvolveSchema(ctx, src.name)
	batch = enBatch(101, 12, true)
	s.enInsert(ctx, src.name, batch, append(append([]string{}, enBaseChildren...), "c"))
	s.enFlush(ctx, src.name)
	src.model = append(src.model, batch...)

	s.enDropSubField(ctx, src.name, "label")
	if withDelete {
		for _, pk := range []int64{1, 7, 103} {
			src.deleted[pk] = true
		}
		resp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
			CollectionName: src.name, Expr: "pk in [1, 7, 103]",
		})
		s.Require().NoError(merr.CheckRPCCall(resp, err))
	}
	batch = enBatch(201, 12, true)
	s.enInsert(ctx, src.name, batch, sebiChildren)
	s.enFlush(ctx, src.name)
	src.model = append(src.model, batch...)
	return src
}

func (s *importTestBase) sebiDescribeInternal(ctx context.Context, name string) *milvuspb.DescribeCollectionResponse {
	resp, err := s.Cluster.MixCoordClient.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection}, CollectionName: name,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	return resp
}

// sebiCreateTarget creates an empty target collection with the same field IDs as
// src by replaying the initial schema and schema evolution.
func (s *importTestBase) sebiCreateTarget(ctx context.Context, src *sebiSource) string {
	target := "sebi_target_" + funcutil.RandomString(8)
	schema := enSchema(target, enBaseChildren)
	schema.EnableDynamicField = true
	s.enCreate(ctx, schema)
	s.sebiDropCleanup(target)
	s.enCreateIndexes(ctx, target)
	s.sebiEvolveSchema(ctx, target)
	s.enDropSubField(ctx, target, "label")
	s.sebiRequireSameFieldIDs(ctx, src.name, target)
	return target
}

// sebiRequireSameFieldIDs requires the target to carry every field, struct and
// sub-field of the source under the same ID, and the source max_field_id.
func (s *importTestBase) sebiRequireSameFieldIDs(ctx context.Context, source, target string) {
	ids := func(schema *schemapb.CollectionSchema) map[string]int64 {
		out := make(map[string]int64)
		for _, field := range schema.GetFields() {
			out[field.GetName()] = field.GetFieldID()
		}
		for _, structField := range schema.GetStructArrayFields() {
			out[structField.GetName()] = structField.GetFieldID()
			for _, field := range structField.GetFields() {
				out[field.GetName()] = field.GetFieldID()
			}
		}
		return out
	}
	maxFieldID := func(resp *milvuspb.DescribeCollectionResponse) string {
		value, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, resp.GetProperties())
		if !ok {
			value, ok = funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, resp.GetSchema().GetProperties())
		}
		s.Require().True(ok, "no %s property", common.MaxFieldIDKey)
		return value
	}
	sourceResp := s.sebiDescribeInternal(ctx, source)
	targetResp := s.sebiDescribeInternal(ctx, target)
	s.Require().Equal(ids(sourceResp.GetSchema()), ids(targetResp.GetSchema()))
	s.Require().Equal(maxFieldID(sourceResp), maxFieldID(targetResp))
}

func (s *importTestBase) sebiImport(ctx context.Context, target string, files []*internalpb.ImportFile, options []*commonpb.KeyValuePair) {
	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: target,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options:        append([]*commonpb.KeyValuePair{enKV(importutilv2.BackupFlag, "true")}, options...),
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.Require().NoError(WaitForImportDone(ctx, s.Cluster, resp.GetJobID()))
}

func (s *importTestBase) sebiSnapshot(ctx context.Context, collection string) string {
	name := "sebi_snapshot_" + funcutil.RandomString(8)
	status, err := s.Cluster.MilvusClient.CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{
		Name: name, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, err := s.Cluster.MilvusClient.DropSnapshot(cleanupCtx, &milvuspb.DropSnapshotRequest{
			Name: name, CollectionName: collection,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	})
	described, err := s.Cluster.MilvusClient.DescribeSnapshot(ctx, &milvuspb.DescribeSnapshotRequest{
		Name: name, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(described, err))
	s.Require().NotEmpty(described.GetS3Location())
	return described.GetS3Location()
}

// sebiCompare requires the imported collection to read back like the source and
// like the model.
func (s *importTestBase) sebiCompare(ctx context.Context, src *sebiSource, target string) {
	s.sebiLoad(ctx, src.name)
	s.sebiLoad(ctx, target)
	want := s.enQueryAll(ctx, src.name)
	s.Require().Equal(src.expected(), want, "the source differs from the model")
	s.Require().Equal(want, s.enQueryAll(ctx, target), "the imported collection differs from the source")
}

type StructEvolutionBinlogImportV2Suite struct {
	importTestBase
}

func (s *StructEvolutionBinlogImportV2Suite) SetupSuite() {
	// Keep the segments written before the evolution as they are. Sort
	// compaction is triggered by auto compaction and index builds wait for
	// it, so it is disabled too.
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportScheduleInterval.Key, "0.1")
	s.importTestBase.SetupSuite()
}

// TestBinlogImport imports StorageV2 segments by their insert_log prefixes.
// The segments written before the struct gained sub-fields have no column
// groups for them, and the segments written before the drop still store the
// dropped sub-field. Deletes are not part of this path: they live in L0
// segments, which a binlog import does not read.
func (s *StructEvolutionBinlogImportV2Suite) TestBinlogImport() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()
	src := s.sebiPrepareSource(ctx, false)
	target := s.sebiCreateTarget(ctx, src)

	segments, err := s.Cluster.ShowSegments(src.name)
	s.Require().NoError(err)
	var files []*internalpb.ImportFile
	for _, segment := range segments {
		if segment.GetState() != commonpb.SegmentState_Flushed || segment.GetLevel() != datapb.SegmentLevel_L1 {
			continue
		}
		s.Require().EqualValues(2, segment.GetStorageVersion())
		files = append(files, &internalpb.ImportFile{Paths: []string{path.Join(s.Cluster.RootPath(), common.SegmentInsertLogPath,
			strconv.FormatInt(segment.GetCollectionID(), 10), strconv.FormatInt(segment.GetPartitionID(), 10),
			strconv.FormatInt(segment.GetID(), 10))}})
	}
	s.Require().NotEmpty(files)
	s.sebiImport(ctx, target, files, []*commonpb.KeyValuePair{enKV(importutilv2.StorageVersion, "2")})
	s.sebiCompare(ctx, src, target)
}

func TestStructEvolutionBinlogImportV2(t *testing.T) {
	suite.Run(t, new(StructEvolutionBinlogImportV2Suite))
}

type StructEvolutionSnapshotImportV3Suite struct {
	importTestBase
}

func (s *StructEvolutionSnapshotImportV3Suite) SetupSuite() {
	// Keep the segments written before the evolution as they are. Sort
	// compaction is triggered by auto compaction and index builds wait for
	// it, so it is disabled too.
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportScheduleInterval.Key, "0.1")
	s.importTestBase.SetupSuite()
}

// TestSnapshotSourceImport imports a snapshot of the source through a
// snapshot-source import, which reads each segment's exact StorageV3 manifest
// and applies the snapshot's deletes.
func (s *StructEvolutionSnapshotImportV3Suite) TestSnapshotSourceImport() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()
	src := s.sebiPrepareSource(ctx, true)
	location := s.sebiSnapshot(ctx, src.name)
	target := s.sebiCreateTarget(ctx, src)
	s.sebiImport(ctx, target, []*internalpb.ImportFile{{Paths: []string{location}}},
		[]*commonpb.KeyValuePair{enKV(importutilv2.SourceType, importutilv2.SourceTypeSnapshot)})
	s.sebiCompare(ctx, src, target)
}

func TestStructEvolutionSnapshotSourceImportV3(t *testing.T) {
	suite.Run(t, new(StructEvolutionSnapshotImportV3Suite))
}
