// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package featureusage

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// TestCompactionTypesAreCounted drives the compaction kinds DataCoord counts.
// Each is triggered by a different user-visible action, and each is counted
// where DataCoord persists the task, not where a DataNode runs it.
//
// The waits are Eventually rather than a fixed sleep because the triggers are
// background loops; the suite shortens their interval so this stays quick.
func (s *Suite) TestCompactionTypesAreCounted() {
	ctx := context.Background()

	// A plain collection: flushing many small segments produces sort
	// compactions, and merging them produces a mix compaction.
	name := "fu_compact_" + funcutil.GenRandomStr()
	s.createSimpleCollection(ctx, name, 8)
	for i := 0; i < 4; i++ {
		s.insertRows(ctx, name, 8, 2000)
		flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
		s.Require().NoError(err)
		s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)
	}
	s.requireCounterEventually(ctx, typeutil.MixCoordRole, "compaction=SortCompaction",
		"flushed segments are sorted")

	// Deletes create level-zero segments; the level-zero trigger compacts them.
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)
	for i := 0; i < 3; i++ {
		// The primary key is auto-generated, so a numeric range over it matches
		// nothing; delete by the scalar field instead.
		del, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
			CollectionName: name,
			Expr:           fmt.Sprintf(`extra like "row-%d%%"`, i),
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, del.GetStatus().GetErrorCode(), del.GetStatus().GetReason())
		flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
		s.Require().NoError(err)
		s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)
	}
	s.requireCounterEventually(ctx, typeutil.MixCoordRole, "compaction=Level0DeleteCompaction",
		"deletes accumulate into level-zero segments")

	// Adding a field bumps the collection's schema version, and the segments
	// written under the old version are rewritten. Two switches gate it, and
	// the policy only selects storage-V3 segments, so the collection for this
	// step is created after both are on.
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key: "true",
		paramtable.Get().CommonCfg.UseLoonFFI.Key:                            "true",
	})
	defer revert()

	bumpName := "fu_bump_" + funcutil.GenRandomStr()
	s.createSimpleCollection(ctx, bumpName, 8)
	alter, err := s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		CollectionName: bumpName,
		Schema: s.mustMarshalField(&schemapb.FieldSchema{
			Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
		}),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, alter.GetErrorCode(), alter.GetReason())
	s.requireCounterEventually(ctx, typeutil.MixCoordRole, "compaction=BumpSchemaVersionCompaction",
		"adding a field rewrites segments written under the old schema version")
}

// TestClusteringCompactionAndSegmentPrune covers the two features that need a
// clustering key: the clustering compaction type, and the QueryNode's
// segment_prune counter, which only fires once the partition statistics that
// compaction produced have reached the delegator.
func (s *Suite) TestClusteringCompactionAndSegmentPrune() {
	ctx := context.Background()
	name := "fu_clustering_" + funcutil.GenRandomStr()
	dim := 8

	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "cluster_key", DataType: schemapb.DataType_Int64, IsClusteringKey: true},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(dim)}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	// Enough rows that clustering compaction emits more than one segment:
	// pruning can only drop a segment when there is another one to keep.
	const rows = 5000
	for b := 0; b < 20; b++ {
		keys := make([]int64, rows)
		for i := range keys {
			keys[i] = int64((b*rows + i) % 1000)
		}
		insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: name,
			FieldsData: []*schemapb.FieldData{
				newInt64Column("cluster_key", keys),
				integration.NewFloatVectorFieldData("vec", rows, dim),
			},
			HashKeys: integration.GenerateHashKeys(rows),
			NumRows:  uint32(rows),
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())
		flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
		s.Require().NoError(err)
		s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)
	}

	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: "vec", IndexName: "_default",
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
	s.WaitForIndexBuilt(ctx, name, "vec")

	compact, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionName:  name,
		MajorCompaction: true,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, compact.GetStatus().GetErrorCode(), compact.GetStatus().GetReason())
	s.requireCounterEventually(ctx, typeutil.MixCoordRole, "compaction=ClusteringCompaction",
		"a major compaction on a clustering key is a clustering compaction")

	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compact.GetCompactionID(),
		})
		return err == nil && state.GetState() == commonpb.CompactionState_Completed
	}, 300*time.Second, 2*time.Second, "clustering compaction should finish")

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)

	// Pruning needs the partition statistics the compaction wrote to reach the
	// delegator, which the leader checker does on its own schedule; retry the
	// search until the counter moves rather than assuming it is already there.
	before := counters(s.report(ctx), typeutil.QueryNodeRole)["segment_prune"]
	s.Require().Eventually(func() bool {
		req := integration.ConstructSearchRequest("", name, "cluster_key == 42", "vec",
			schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"nprobe": 4}, 1, dim, 5, -1)
		req.UseDefaultConsistency = true
		if _, err := s.Cluster.MilvusClient.Search(ctx, req); err != nil {
			return false
		}
		return counters(s.report(ctx), typeutil.QueryNodeRole)["segment_prune"] > before
	}, 180*time.Second, 3*time.Second, "a clustering-key filter should prune segments")
}

// requireCounterEventually waits for a counter to become non-zero, so a test
// does not depend on how fast a background trigger runs.
func (s *Suite) requireCounterEventually(ctx context.Context, role, counter, why string) {
	s.Require().Eventuallyf(func() bool {
		return counters(s.report(ctx), role)[counter] > 0
	}, 300*time.Second, 3*time.Second, "%s should have been counted: %s", counter, why)
}

func newInt64Column(name string, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

// mustMarshalField encodes one field for AddCollectionField, which takes the
// new field as a serialized FieldSchema rather than a whole collection schema.
func (s *Suite) mustMarshalField(field *schemapb.FieldSchema) []byte {
	b, err := proto.Marshal(field)
	s.Require().NoError(err)
	return b
}

// createPartitionKeyCollection builds a collection whose segments are sorted
// by a partition key rather than by the primary key.
func (s *Suite) createPartitionKeyCollection(ctx context.Context, name string, dim int) {
	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "tenant", DataType: schemapb.DataType_VarChar, IsPartitionKey: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(dim)}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1, NumPartitions: 4,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	const rows = 3000
	for b := 0; b < 3; b++ {
		tenants := make([]string, rows)
		for i := range tenants {
			tenants[i] = fmt.Sprintf("t%d", (b*rows+i)%8)
		}
		insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: name,
			FieldsData: []*schemapb.FieldData{
				newVarCharColumn("tenant", tenants),
				integration.NewFloatVectorFieldData("vec", rows, dim),
			},
			HashKeys: integration.GenerateHashKeys(rows),
			NumRows:  uint32(rows),
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())
		flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
		s.Require().NoError(err)
		s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)
	}
}
