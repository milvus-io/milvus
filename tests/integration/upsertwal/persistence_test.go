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

package upsertwal

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *UpsertWALSuite) TestChunkedUpsertHasOneLogicalEffect() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	const rows = 32768
	msg := c.upsert(11, 2, "chunked")
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	keys, rowIDs, markers := make([]int64, rows), make([]int64, rows), make([]int64, rows)
	vectors := make([]float32, rows*2)
	offsets := make([]uint32, rows)
	for index := range rows {
		keys[index], rowIDs[index], markers[index] = int64(index+11), (1<<62)+rowSequence.Add(1), 2
		vectors[2*index], vectors[2*index+1] = 1, 2
		offsets[index] = uint32(index)
	}
	for _, field := range body.Insert.FieldsData {
		switch field.GetFieldName() {
		case "pk":
			field.GetScalars().GetLongData().Data = keys
		case "marker":
			field.GetScalars().GetLongData().Data = markers
		case "vector":
			field.GetVectors().GetFloatVector().Data = vectors
		}
	}
	body.Insert.NumRows, body.Insert.RowIDs = rows, rowIDs
	body.Delete.NumRows, body.Delete.PrimaryKeys.GetIntId().Data = rows, keys
	typed.OverwriteBody(body)
	header := typed.Header()
	header.IdempotentResult.RowOffsets, header.IdempotentResult.Ids.GetIntId().Data = offsets, keys
	header.DeleteRows, header.Partition.Rows, header.Partition.BinarySize = rows, rows, rows*32
	typed.OverwriteHeader(header)
	s.Require().Greater(msg.EstimateSize(), 524288, "input must exceed the configured physical message limit")
	result, err := producer.Append(ctx, msg)
	s.Require().NoError(err)
	s.Require().NotNil(result.MessageID)
	s.Require().Positive(result.TimeTick)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertRows(ctx, c, rows+10, 2, 1)
	s.flush(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}

func (s *UpsertWALSuite) TestCompactionPreservesReplacement() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	_, err = producer.Append(ctx, c.upsert(12, 9))
	s.Require().NoError(err)
	s.flush(ctx, c)
	_, err = producer.Append(ctx, c.upsert(11, 2))
	s.Require().NoError(err)
	s.flush(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.WaitForIndexBuilt(ctx, c.name, "vector")
	compaction, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: c.id, L0Compaction: true})
	s.Require().NoError(merr.CheckRPCCall(compaction, err))
	s.Require().Positive(compaction.GetCompactionID())
	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: compaction.GetCompactionID()})
		return merr.CheckRPCCall(state, err) == nil && state.GetState() == commonpb.CompactionState_Completed
	}, 2*time.Minute, 100*time.Millisecond)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertRows(ctx, c, 12, 9, 1)
}

func (s *UpsertWALSuite) TestReplacementSurvivesFlushRestartAndReload() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	s.flush(ctx, c)
	_, err = producer.Append(ctx, c.upsert(11, 2))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.flush(ctx, c)
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	producer.Close()
	s.restartStreamingNode()
	queryNodes := s.Cluster.GetAllQueryNodes()
	s.Require().Len(queryNodes, 1)
	_ = queryNodes[0].Kill()
	s.Cluster.AddQueryNode()
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	s.waitForQueryRecovery(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}
