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

package compaction

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type BloomSizingReadbackSuite struct {
	integration.MiniClusterSuite
	storageVersion int64
}

func (s *BloomSizingReadbackSuite) SetupSuite() {
	p := paramtable.Get()
	s.WithMilvusConfig(p.CommonCfg.UseLoonFFI.Key, strconv.FormatBool(s.storageVersion == 3))

	s.WithMilvusConfig(p.QuotaConfig.FlushLimitEnabled.Key, "false")
	s.WithMilvusConfig(p.CommonCfg.BloomFilterType.Key, "BlockedBloomFilter")
	s.WithMilvusConfig(p.DataCoordCfg.EnableAutoCompaction.Key, "true")
	s.WithMilvusConfig(p.DataCoordCfg.MixCompactionTriggerInterval.Key, "3600")
	s.WithMilvusConfig(p.DataCoordCfg.L0CompactionTriggerInterval.Key, "1")
	s.WithMilvusConfig(p.DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.Key, "1")
	s.WithMilvusConfig(p.DataCoordCfg.SingleCompactionRatioThreshold.Key, "1.0")
	s.WithMilvusConfig(p.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key, "99999")
	s.WithMilvusConfig(p.DataCoordCfg.SingleCompactionDeltaLogMaxSize.Key, "999999999999")
	s.WithMilvusConfig(p.DataCoordCfg.SegmentMaxSize.Key, "1")
	s.WithMilvusConfig(p.RootCoordCfg.DmlChannelNum.Key, "1")
	s.MiniClusterSuite.SetupSuite()
}

// Exercise both PK encodings through real target-size compaction and reload.
func (s *BloomSizingReadbackSuite) TestTargetSizeReadback() {
	for _, varchar := range []bool{false, true} {
		name := "Int64"
		if varchar {
			name = "VarChar"
		}
		s.Run(name, func() { s.runBloomReadback(varchar) })
	}
}

func (s *BloomSizingReadbackSuite) runBloomReadback(varchar bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	c := s.Cluster
	name := "BloomReadback_" + funcutil.GenRandomStr()
	const batch, batches, dim = 128, 3, 32
	const preDeleted = batch * batches / 2
	schema := integration.ConstructSchemaOfVecDataType(name, dim, false, schemapb.DataType_FloatVector)
	if varchar {
		schema.Fields[0].DataType = schemapb.DataType_VarChar
		schema.Fields[0].TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "20"}}
	}
	literal := func(pk int) string {
		if varchar {
			return fmt.Sprintf("%q", fmt.Sprintf("%020d", pk))
		}
		return strconv.Itoa(pk)
	}
	encoded, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: name, Schema: encoded, ShardsNum: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{CollectionName: name, FieldName: integration.FloatVecField, IndexName: "vec", ExtraParams: integration.ConstructIndexParam(dim, "FLAT", "L2")})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer func() {
		status, err := c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
	}()
	for i := 0; i < batches; i++ {
		pkData := integration.NewInt64FieldDataWithStart(integration.Int64Field, batch, int64(i*batch))
		if varchar {
			values := make([]string, batch)
			for j := range values {
				values[j] = fmt.Sprintf("%020d", i*batch+j)
			}
			pkData = &schemapb.FieldData{Type: schemapb.DataType_VarChar, FieldName: integration.Int64Field, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}}
		}
		result, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: name, NumRows: uint32(batch), FieldsData: []*schemapb.FieldData{pkData, integration.NewFloatVectorFieldData(integration.FloatVecField, batch, dim)}, HashKeys: integration.GenerateHashKeys(batch)})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		flushed, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
		s.Require().NoError(merr.CheckRPCCall(flushed, err))
		s.WaitForFlush(ctx, flushed.GetFlushCollSegIDs()[name].GetData(), flushed.GetCollFlushTs()[name], "", name)
	}

	s.Require().Eventually(func() bool {
		segments, err := c.ShowSegments(name)
		if err != nil {
			return false
		}
		inputCount := 0
		var count int64
		for _, seg := range segments {
			if seg.GetState() != commonpb.SegmentState_Flushed {
				continue
			}
			if !seg.GetIsSorted() {
				return false
			}
			inputCount++
			count += seg.GetNumOfRows()
		}
		return inputCount >= 2 && count == int64(batch*batches)
	}, 3*time.Minute, time.Second, "wait for sorted input segments")
	desc, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	s.WaitForIndexBuilt(ctx, name, integration.FloatVecField)
	status, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)

	deleted, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{CollectionName: name, Expr: fmt.Sprintf("%s < %s", integration.Int64Field, literal(preDeleted))})
	s.Require().NoError(merr.CheckRPCCall(deleted, err))
	flushed, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flushed, err))
	s.WaitForFlush(ctx, flushed.GetFlushCollSegIDs()[name].GetData(), flushed.GetCollFlushTs()[name], "", name)

	s.Require().Eventually(func() bool {
		segments, err := c.ShowSegments(name)
		if err != nil {
			return false
		}
		var deltaRows int64
		for _, seg := range segments {
			if seg.GetState() != commonpb.SegmentState_Flushed {
				continue
			}
			if seg.GetLevel() == datapb.SegmentLevel_L0 {
				return false
			}
			if seg.GetStorageVersion() == 3 {
				// V3 persists delta counts in Stats and paths in its manifest,
				// not in the per-field etcd binlog keys read by ShowSegments.
				deltaRows += seg.GetStats().GetDeleteNumRows()
			} else {
				for _, field := range seg.GetDeltalogs() {
					for _, log := range field.GetBinlogs() {
						deltaRows += log.GetEntriesNum()
					}
				}
			}
		}
		return deltaRows >= int64(preDeleted)
	}, 3*time.Minute, time.Second, "wait for L0 deletes to reach sorted input segments")

	compacted, err := c.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: desc.GetCollectionID(), TargetSize: 1})
	s.Require().NoError(merr.CheckRPCCall(compacted, err))
	s.Require().Eventually(func() bool {
		state, err := c.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: compacted.GetCompactionID()})
		return err == nil && state.GetState() == commonpb.CompactionState_Completed && state.GetCompletedPlanNo() > 0
	}, 3*time.Minute, time.Second, "manual target_size compaction must complete a plan")
	segments, err := c.ShowSegments(name)
	s.Require().NoError(err)
	merged := false
	var compactedRows int64
	for _, seg := range segments {
		if seg.GetState() == commonpb.SegmentState_Flushed && len(seg.GetCompactionFrom()) > 1 {
			merged = true
			compactedRows += seg.GetNumOfRows()
			s.Require().True(seg.GetIsSorted())
			s.Require().Equal(s.storageVersion, seg.GetStorageVersion())
		}
	}
	s.Require().True(merged, "must observe a sorted multi-input output")
	s.Require().Equal(int64(batch*batches-preDeleted), compactedRows, "compaction must physically remove deleted rows")
	check := func(start int) {
		result, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{CollectionName: name, Expr: fmt.Sprintf("%s >= %s", integration.Int64Field, literal(0)), OutputFields: []string{integration.Int64Field}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		got := result.GetFieldsData()[0].GetScalars().GetLongData().GetData()
		if varchar {
			for _, v := range result.GetFieldsData()[0].GetScalars().GetStringData().GetData() {
				pk, err := strconv.ParseInt(v, 10, 64)
				s.Require().NoError(err)
				got = append(got, pk)
			}
		}
		want := make([]int64, batch*batches-start)
		for i := range want {
			want[i] = int64(start + i)
		}
		s.Require().ElementsMatch(want, got)
	}
	reload := func() {
		status, err := c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		status, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForLoad(ctx, name)
	}
	reload()
	check(preDeleted)
	deleted, err = c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{CollectionName: name, Expr: fmt.Sprintf("%s < %s", integration.Int64Field, literal(preDeleted+64))})
	s.Require().NoError(merr.CheckRPCCall(deleted, err))
	check(preDeleted + 64)
	flushed, err = c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flushed, err))
	s.WaitForFlush(ctx, flushed.GetFlushCollSegIDs()[name].GetData(), flushed.GetCollFlushTs()[name], "", name)
	reload()
	check(preDeleted + 64)
}

func TestBloomSizingReadback(t *testing.T) {
	for _, version := range []int64{2, 3} {
		t.Run(fmt.Sprintf("V%d", version), func(t *testing.T) {
			suite.Run(t, &BloomSizingReadbackSuite{storageVersion: version})
		})
	}
}
