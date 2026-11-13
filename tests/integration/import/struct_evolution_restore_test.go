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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/schemautil"
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

// The struct evolution restore suites back up and restore a collection whose
// struct evolved after data was written, through the three restore paths a
// backup tool uses:
//   - binlog import of StorageV2 segments into a collection created with
//     preserve_field_ids (TestBinlogImport);
//   - snapshot-source import of StorageV3 segments into a collection created
//     with preserve_field_ids (TestSnapshotSourceImport);
//   - RestoreSnapshot, which copies the segments (TestRestoreSnapshot).
//
// The source struct holds a legacy Int64 sub-field, an element-nullable
// VarChar, a nested Array<Array<Int64>>, an element-nullable and a legacy
// FloatVector. After the first batch it gains an element-nullable Int64 and an
// element-nullable FloatVector sub-field, and later loses the VarChar one. The
// restored collection must read back every row like the source and like the
// model the rows were built from.

// seAddedVector is the element-nullable vector sub-field added to the struct.
// Rows never carry it, so it reads back as one null per element.
const seAddedVector = "av"

// seChildren are the sub-fields after the evolution, without seAddedVector.
var seChildren = []string{"id", "tags", "nvec", "vec", "c"}

func seAddedVectorSchema() *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name:     typeutil.ConcatStructFieldName(enStruct, seAddedVector),
		DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
		Nullable: true, ElementNullable: true,
		TypeParams: []*commonpb.KeyValuePair{enKV(common.MaxCapacityKey, "10"), enKV(common.DimKey, strconv.Itoa(enDim))},
	}
}

type seSource struct {
	name    string
	model   []enRow
	deleted map[int64]bool
}

// expected is the query output of the source after the evolution.
func (src *seSource) expected() map[int64]map[string]any {
	rows := make([]enRow, 0, len(src.model))
	for _, row := range src.model {
		if !src.deleted[row.pk] {
			rows = append(rows, row)
		}
	}
	out := enExpectedRows(rows, seChildren)
	for _, row := range rows {
		if row.null {
			out[row.pk][seAddedVector] = nil
			continue
		}
		out[row.pk][seAddedVector] = make([]any, len(row.elems))
	}
	return out
}

func (s *importTestBase) seCreateAddedVectorIndex(ctx context.Context, name string) {
	field := typeutil.ConcatStructFieldName(enStruct, seAddedVector)
	status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: field, IndexName: "idx_added_vector",
		ExtraParams: integration.ConstructIndexParam(enDim, integration.IndexHNSW, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err), field)
}

func (s *importTestBase) seLoad(ctx context.Context, name string) {
	s.WaitForIndexBuilt(ctx, name, typeutil.ConcatStructFieldName(enStruct, seAddedVector))
	s.enLoad(ctx, name)
}

// sePrepareSource creates the source collection, writes three batches around
// the struct evolution and, when withDelete is set, deletes rows written
// before the last batch.
func (s *importTestBase) sePrepareSource(ctx context.Context, withDelete bool) *seSource {
	src := &seSource{name: "se_source_" + funcutil.RandomString(8), deleted: map[int64]bool{}}
	schema := enSchema(src.name, enBaseChildren)
	schema.EnableDynamicField = true
	s.enCreate(ctx, schema)
	s.enCreateIndexes(ctx, src.name)

	batch := enBatch(1, 24, false)
	s.enInsert(ctx, src.name, batch, enBaseChildren)
	s.enFlush(ctx, src.name)
	src.model = append(src.model, batch...)

	s.enAddSubField(ctx, src.name, "c")
	s.enAlter(ctx, src.name, &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
		AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
			FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{FieldSchema: seAddedVectorSchema()}},
		},
	}})
	s.seCreateAddedVectorIndex(ctx, src.name)
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
	s.enInsert(ctx, src.name, batch, seChildren)
	s.enFlush(ctx, src.name)
	src.model = append(src.model, batch...)
	return src
}

func (s *importTestBase) seDescribeInternal(ctx context.Context, name string) *milvuspb.DescribeCollectionResponse {
	resp, err := s.Cluster.MixCoordClient.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection}, CollectionName: name,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	return resp
}

// seCreateTarget creates the restore target the way a backup tool does: the
// schema DescribeCollection returns for the source, with bare sub-field names
// and source field IDs, the source $meta field ID, and the source properties,
// which carry max_field_id, plus preserve_field_ids=true.
func (s *importTestBase) seCreateTarget(ctx context.Context, src *seSource) string {
	described, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: src.name})
	s.Require().NoError(merr.CheckRPCCall(described.GetStatus(), err))
	target := "se_target_" + funcutil.RandomString(8)
	schema := proto.Clone(described.GetSchema()).(*schemapb.CollectionSchema)
	schema.Name = target
	var metaID int64
	for _, field := range s.seDescribeInternal(ctx, src.name).GetSchema().GetFields() {
		if field.GetIsDynamic() {
			metaID = field.GetFieldID()
		}
	}
	s.Require().NotZero(metaID)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: metaID, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true,
	})
	properties := append([]*commonpb.KeyValuePair{}, described.GetProperties()...)
	properties = append(properties, enKV(util.PreserveFieldIdsKey, "true"))
	data, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: target, Schema: data, ShardsNum: described.GetShardsNum(), Properties: properties,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.seRequireSameFieldIDs(ctx, src.name, target, false)
	s.enCreateIndexes(ctx, target)
	s.seCreateAddedVectorIndex(ctx, target)
	return target
}

// seRequireSameFieldIDs requires the target to carry every field, struct and
// sub-field of the source under the same ID, the source max_field_id, and,
// when keepsMarkers is set, the origin marker of the added sub-fields.
func (s *importTestBase) seRequireSameFieldIDs(ctx context.Context, source, target string, keepsMarkers bool) {
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
	sourceResp := s.seDescribeInternal(ctx, source)
	targetResp := s.seDescribeInternal(ctx, target)
	s.Require().Equal(ids(sourceResp.GetSchema()), ids(targetResp.GetSchema()))
	// The IDs of dropped fields and sub-fields stay reserved.
	s.Require().Equal(maxFieldID(sourceResp), maxFieldID(targetResp))
	for _, structField := range targetResp.GetSchema().GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			name, err := typeutil.ExtractStructFieldName(field.GetName())
			s.Require().NoError(err)
			added := name == "c" || name == seAddedVector
			s.Require().Equal(added && keepsMarkers, schemautil.IsAddedStructSubField(field), field.GetName())
		}
	}
}

func (s *importTestBase) seImport(ctx context.Context, target string, files []*internalpb.ImportFile, options []*commonpb.KeyValuePair) {
	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: target,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options:        append([]*commonpb.KeyValuePair{enKV(importutilv2.BackupFlag, "true")}, options...),
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.Require().NoError(WaitForImportDone(ctx, s.Cluster, resp.GetJobID()))
}

func (s *importTestBase) seSnapshot(ctx context.Context, collection string) (string, string) {
	name := "se_snapshot_" + funcutil.RandomString(8)
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
	return name, described.GetS3Location()
}

// seCompare requires the restored collection to read back like the source and
// like the model.
func (s *importTestBase) seCompare(ctx context.Context, src *seSource, target string) {
	s.seLoad(ctx, src.name)
	s.seLoad(ctx, target)
	want := s.seQueryAll(ctx, src.name)
	s.Require().Equal(src.expected(), want, "the source differs from the model")
	s.Require().Equal(want, s.seQueryAll(ctx, target), "the restored collection differs from the source")
}

func (s *importTestBase) seQueryAll(ctx context.Context, name string) map[int64]map[string]any {
	return s.enQueryAll(ctx, name)
}

type StructEvolutionRestoreV2Suite struct {
	importTestBase
}

func (s *StructEvolutionRestoreV2Suite) SetupSuite() {
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

// TestBinlogImport restores StorageV2 segments by their insert_log prefixes.
// The segments written before the struct gained sub-fields have no column
// groups for them, and the segments written before the drop still store the
// dropped sub-field. Deletes are not part of this path: they live in L0
// segments, which a binlog import does not read.
func (s *StructEvolutionRestoreV2Suite) TestBinlogImport() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()
	src := s.sePrepareSource(ctx, false)
	target := s.seCreateTarget(ctx, src)

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
	s.seImport(ctx, target, files, []*commonpb.KeyValuePair{enKV(importutilv2.StorageVersion, "2")})
	s.seCompare(ctx, src, target)
}

func TestStructEvolutionRestoreV2(t *testing.T) {
	suite.Run(t, new(StructEvolutionRestoreV2Suite))
}

type StructEvolutionRestoreV3Suite struct {
	importTestBase
}

func (s *StructEvolutionRestoreV3Suite) SetupSuite() {
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

// TestSnapshotSourceImport restores a snapshot of the source through a
// snapshot-source import, which reads each segment's exact StorageV3 manifest
// and applies the snapshot's deletes.
func (s *StructEvolutionRestoreV3Suite) TestSnapshotSourceImport() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()
	src := s.sePrepareSource(ctx, true)
	_, location := s.seSnapshot(ctx, src.name)
	target := s.seCreateTarget(ctx, src)
	s.seImport(ctx, target, []*internalpb.ImportFile{{Paths: []string{location}}},
		[]*commonpb.KeyValuePair{enKV(importutilv2.SourceType, importutilv2.SourceTypeSnapshot)})
	s.seCompare(ctx, src, target)
}

// TestRestoreSnapshot restores a snapshot of the source with RestoreSnapshot,
// which keeps the stored schema, including the origin marker of the added
// sub-fields, and copies the segments as they are.
func (s *StructEvolutionRestoreV3Suite) TestRestoreSnapshot() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 20*time.Minute)
	defer cancel()
	src := s.sePrepareSource(ctx, true)
	snapshot, _ := s.seSnapshot(ctx, src.name)
	target := "se_restored_" + funcutil.RandomString(8)
	resp, err := s.Cluster.MilvusClient.RestoreSnapshot(ctx, &milvuspb.RestoreSnapshotRequest{
		Name: snapshot, CollectionName: src.name, TargetCollectionName: target,
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	for {
		state, err := s.Cluster.MilvusClient.GetRestoreSnapshotState(ctx, &milvuspb.GetRestoreSnapshotStateRequest{JobId: resp.GetJobId()})
		s.Require().NoError(merr.CheckRPCCall(state, err))
		s.Require().NotEqual(milvuspb.RestoreSnapshotState_RestoreSnapshotFailed, state.GetInfo().GetState(), state.GetInfo().GetReason())
		if state.GetInfo().GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted {
			break
		}
		s.Require().NoError(ctx.Err(), "restore did not complete")
		time.Sleep(time.Second)
	}
	s.seRequireSameFieldIDs(ctx, src.name, target, true)
	s.seCompare(ctx, src, target)
}

func TestStructEvolutionRestoreV3(t *testing.T) {
	suite.Run(t, new(StructEvolutionRestoreV3Suite))
}
