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

package commit_timestamp

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const dim = 128

func TestCommitTimestampSuite(t *testing.T) {
	suite.Run(t, new(CommitTimestampSuite))
}

type CommitTimestampSuite struct {
	integration.MiniClusterSuite
}

// ─── Helpers ──────────────────────────────────────────────────────────────

// setCommitTimestamp reads a segment's metadata from etcd, sets its
// CommitTimestamp to commitTs, then writes it back. This simulates an
// import segment without going through the actual import pipeline.
func (s *CommitTimestampSuite) setCommitTimestamp(
	collectionID int64,
	commitTs uint64,
) []int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := path.Join(s.Cluster.RootPath(), "meta/datacoord-meta/s",
		fmt.Sprintf("%d", collectionID)) + "/"

	resp, err := s.Cluster.EtcdCli.Get(ctx, prefix, clientv3.WithPrefix())
	s.Require().NoError(err, "failed to list segments from etcd")
	s.Require().NotEmpty(resp.Kvs, "no segments found in etcd")

	var segmentIDs []int64
	for _, kv := range resp.Kvs {
		var seg datapb.SegmentInfo
		err := proto.Unmarshal(kv.Value, &seg)
		if err != nil {
			continue
		}
		if seg.GetState() != commonpb.SegmentState_Flushed && seg.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		if len(seg.GetBinlogs()) == 0 {
			continue
		}

		log.Info("setCommitTimestamp: modifying segment",
			zap.Int64("segmentID", seg.GetID()),
			zap.Uint64("commitTs", commitTs))

		seg.CommitTimestamp = commitTs

		data, err := proto.Marshal(&seg)
		s.Require().NoError(err)
		_, err = s.Cluster.EtcdCli.Put(ctx, string(kv.Key), string(data))
		s.Require().NoError(err)
		segmentIDs = append(segmentIDs, seg.GetID())
	}
	s.Require().NotEmpty(segmentIDs, "no flushed segments were modified")
	return segmentIDs
}

// createCollectionAndInsert creates a collection, inserts rows, and flushes.
// Returns (collectionName, collectionID).
func (s *CommitTimestampSuite) createCollectionAndInsert(
	ctx context.Context,
	rowNum int,
) (string, int64) {
	collName := "CommitTs_" + funcutil.RandomString(6)

	schema := integration.ConstructSchema(collName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp))

	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 1)
	vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collName,
		FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(insertResp.GetStatus()))

	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(flushResp.GetStatus()))
	segIDs := flushResp.GetCollSegIDs()[collName].GetData()
	flushTs := flushResp.GetCollFlushTs()[collName]
	s.WaitForFlush(ctx, segIDs, flushTs, "", collName)

	showResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(showResp.GetStatus()))
	collectionID := showResp.GetCollectionIds()[0]

	return collName, collectionID
}

// buildIndexAndLoad creates an index and loads the collection.
func (s *CommitTimestampSuite) buildIndexAndLoad(ctx context.Context, collName string) {
	indexResp, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collName,
		FieldName:      integration.FloatVecField,
		IndexName:      "vec_idx",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(indexResp))
	s.WaitForIndexBuilt(ctx, collName, integration.FloatVecField)

	loadResp, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collName,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(loadResp))
	s.WaitForLoad(ctx, collName)
}

// queryCountWithTs queries count(*) with an explicit guarantee timestamp.
// Use guaranteeTs=0 for strong consistency.
func (s *CommitTimestampSuite) queryCountWithTs(ctx context.Context, collName string, guaranteeTs uint64) int64 {
	req := &milvuspb.QueryRequest{
		CollectionName: collName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
		QueryParams: []*commonpb.KeyValuePair{
			{Key: "reduce_stop_for_best", Value: "false"},
		},
	}
	if guaranteeTs > 0 {
		req.GuaranteeTimestamp = guaranteeTs
	} else {
		req.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	}

	queryResp, err := s.Cluster.MilvusClient.Query(ctx, req)
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())

	for _, field := range queryResp.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			return field.GetScalars().GetLongData().GetData()[0]
		}
	}
	s.Fail("count(*) field not found in query response")
	return 0
}

// deleteByPKs deletes the first N PKs (1-based) from the collection.
func (s *CommitTimestampSuite) deleteByPKs(ctx context.Context, collName string, count int) {
	pks := make([]string, count)
	for i := 0; i < count; i++ {
		pks[i] = strconv.FormatInt(int64(i+1), 10)
	}
	expr := fmt.Sprintf("%s in [%s]", integration.Int64Field, strings.Join(pks, ","))
	deleteResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collName,
		Expr:           expr,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(deleteResp.GetStatus()))
}

// searchWithTs performs a search with explicit guarantee timestamp. Returns result count.
func (s *CommitTimestampSuite) searchWithTs(ctx context.Context, collName string, guaranteeTs uint64, topk int) int {
	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collName, "",
		integration.FloatVecField, schemapb.DataType_FloatVector, nil,
		metric.L2, params, 1, dim, topk, -1)
	searchReq.GuaranteeTimestamp = guaranteeTs

	searchResult, err := s.Cluster.MilvusClient.Search(ctx, searchReq)
	s.Require().NoError(err)
	s.Require().True(merr.Ok(searchResult.GetStatus()), searchResult.GetStatus().GetReason())

	return len(searchResult.GetResults().GetScores())
}

// ─── MVCC Visibility ──────────────────────────────────────────────────────

func (s *CommitTimestampSuite) TestMVCC_Visibility() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	tBefore := tsoutil.ComposeTSByTime(time.Now(), 0)

	// Set commit_ts to a future time to test MVCC
	tCommit := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	tAfterCommit := tsoutil.ComposeTSByTime(time.Now().Add(20*time.Second), 0)
	s.setCommitTimestamp(collectionID, tCommit)

	s.buildIndexAndLoad(ctx, collName)

	// Query with guarantee_ts < commit_ts → 0 rows
	count := s.queryCountWithTs(ctx, collName, tBefore)
	s.Equal(int64(0), count,
		"MVCC: query before commit_ts should return 0 rows")

	// Strong consistency query (guarantee_ts = now < commit_ts) → 0 rows
	count = s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(0), count,
		"MVCC: strong consistency query should return 0 rows when commit_ts is in the future")

	// Query with guarantee_ts = commit_ts → all rows
	count = s.queryCountWithTs(ctx, collName, tCommit)
	s.Equal(int64(rowNum), count,
		"MVCC: query at commit_ts should return all rows")

	// Query with guarantee_ts > commit_ts → all rows
	count = s.queryCountWithTs(ctx, collName, tAfterCommit)
	s.Equal(int64(rowNum), count,
		"MVCC: query after commit_ts should return all rows")
}

func (s *CommitTimestampSuite) TestMVCC_StrongConsistency_CommitTsInPast() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// Set commit_ts to now (in the past by the time query runs)
	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Strong consistency query should return all rows when commit_ts is in the past
	count := s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowNum), count,
		"MVCC: strong consistency query should return all rows when commit_ts is in the past")
}

// ─── Search ──────────────────────────────────────────────────────────────

func (s *CommitTimestampSuite) TestSearch_WithGuaranteeTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	tBefore := tsoutil.ComposeTSByTime(time.Now(), 0)
	tCommit := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	s.setCommitTimestamp(collectionID, tCommit)

	s.buildIndexAndLoad(ctx, collName)

	// Search with guarantee_ts < commit_ts → 0 results
	resultCount := s.searchWithTs(ctx, collName, tBefore, 10)
	s.Equal(0, resultCount,
		"search before commit_ts should return 0 results")

	// Search with guarantee_ts = commit_ts → results
	resultCount = s.searchWithTs(ctx, collName, tCommit, 10)
	s.Greater(resultCount, 0,
		"search at commit_ts should return results")
}

// ─── Delete ──────────────────────────────────────────────────────────────

func (s *CommitTimestampSuite) TestDelete_AfterCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const deleteCount = 10

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// commit_ts in the past so delete_ts > commit_ts
	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Verify all rows present
	count := s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowNum), count, "should have all rows before delete")

	s.deleteByPKs(ctx, collName, deleteCount)
	time.Sleep(2 * time.Second)

	// Delete after commit_ts should take effect
	count = s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowNum-deleteCount), count,
		"delete after commit_ts should take effect")
}

func (s *CommitTimestampSuite) TestDelete_BeforeCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const deleteCount = 10

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// commit_ts in the future so delete_ts < commit_ts
	commitTs := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	s.deleteByPKs(ctx, collName, deleteCount)
	time.Sleep(2 * time.Second)

	// Delete before commit_ts should NOT take effect — query at commit_ts
	count := s.queryCountWithTs(ctx, collName, commitTs)
	s.Equal(int64(rowNum), count,
		"delete before commit_ts should not take effect")
}

// ─── Upsert ──────────────────────────────────────────────────────────────

func (s *CommitTimestampSuite) TestUpsert_AfterCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const upsertCount = 20

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// commit_ts in the past so upsert_ts > commit_ts
	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Upsert first 20 rows
	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, upsertCount, 1)
	vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, upsertCount, dim)
	upsertResp, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: collName,
		FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
		NumRows:        uint32(upsertCount),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(upsertResp.GetStatus()))

	time.Sleep(2 * time.Second)

	// After upsert, total count should remain the same (old rows deleted, new rows inserted)
	count := s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowNum), count,
		"after upsert, total row count should remain %d", rowNum)

	// Validate upsert worked: query the upserted PKs — they should exist
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collName,
		Expr:             fmt.Sprintf("%s in [1,2,3]", integration.Int64Field),
		OutputFields:     []string{integration.Int64Field},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()))
	s.Equal(3, len(queryResp.GetFieldsData()[0].GetScalars().GetLongData().GetData()),
		"upserted PKs should be queryable")
}

func (s *CommitTimestampSuite) TestUpsert_BeforeCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const upsertCount = 20

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// commit_ts in the future so upsert_ts < commit_ts
	commitTs := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, upsertCount, 1)
	vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, upsertCount, dim)
	upsertResp, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: collName,
		FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
		NumRows:        uint32(upsertCount),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(upsertResp.GetStatus()))

	time.Sleep(2 * time.Second)

	// Upsert before commit_ts: the delete part should not take effect on the
	// import segment (row didn't exist yet at upsert time), so we should see
	// rowNum + upsertCount rows at commit_ts.
	count := s.queryCountWithTs(ctx, collName, commitTs)
	s.Equal(int64(rowNum+upsertCount), count,
		"upsert before commit_ts: delete part should not apply, expect %d rows", rowNum+upsertCount)
}

// ─── Compaction ──────────────────────────────────────────────────────────

func (s *CommitTimestampSuite) TestCompaction_NormalizesCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowsPerSegment = 50

	collName := "CommitTs_Compact_" + funcutil.RandomString(6)

	schema := integration.ConstructSchema(collName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp))

	// Insert two batches to create two segments
	for batch := 0; batch < 2; batch++ {
		startPK := int64(batch*rowsPerSegment + 1)
		pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowsPerSegment, startPK)
		vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowsPerSegment, dim)
		insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collName,
			FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
			NumRows:        uint32(rowsPerSegment),
		})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(insertResp.GetStatus()))

		flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			CollectionNames: []string{collName},
		})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(flushResp.GetStatus()))
		segIDs := flushResp.GetCollSegIDs()[collName].GetData()
		flushTs := flushResp.GetCollFlushTs()[collName]
		s.WaitForFlush(ctx, segIDs, flushTs, "", collName)
	}

	showResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	collectionID := showResp.GetCollectionIds()[0]

	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	modifiedSegIDs := s.setCommitTimestamp(collectionID, commitTs)
	s.Require().GreaterOrEqual(len(modifiedSegIDs), 2,
		"should have at least 2 segments to compact")

	// Trigger compaction
	compactResp, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: collectionID,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(compactResp.GetStatus()))

	compactionID := compactResp.GetCompactionID()

	compactionCompleted := false
	for i := 0; i < 60; i++ {
		time.Sleep(2 * time.Second)
		stateResp, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compactionID,
		})
		if err != nil {
			continue
		}
		if stateResp.GetState() == commonpb.CompactionState_Completed {
			log.Info("compaction completed", zap.Int64("compactionID", compactionID))
			compactionCompleted = true
			break
		}
	}
	s.Require().True(compactionCompleted, "compaction did not complete within timeout")

	// Verify output segments: CommitTimestamp = 0, binlog timestamps updated
	segments, err := s.Cluster.ShowSegments(collName)
	s.Require().NoError(err)

	for _, seg := range segments {
		if seg.GetState() == commonpb.SegmentState_Flushed {
			s.Equal(uint64(0), seg.GetCommitTimestamp(),
				"compaction output segment %d must have CommitTimestamp=0 (normalized)", seg.GetID())

			// Verify binlog TimestampFrom/To are reasonable (not stale)
			for _, fieldBinlog := range seg.GetBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					s.GreaterOrEqual(binlog.GetTimestampFrom(), commitTs,
						"compaction output binlog TimestampFrom should be >= commitTs")
					s.GreaterOrEqual(binlog.GetTimestampTo(), commitTs,
						"compaction output binlog TimestampTo should be >= commitTs")
				}
			}
		}
	}

	// Verify data is still queryable after compaction
	s.buildIndexAndLoad(ctx, collName)
	count := s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowsPerSegment*2), count,
		"all rows should be queryable after compaction normalization")

	// Verify delete works normally after compaction (segment is now normal)
	s.deleteByPKs(ctx, collName, 5)
	time.Sleep(2 * time.Second)
	count = s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowsPerSegment*2-5), count,
		"delete should work normally on compacted (normalized) segment")
}

// ─── GC Protection ──────────────────────────────────────────────────────

func (s *CommitTimestampSuite) TestGC_ImportSegmentNotPrematurelyDropped() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// Set commit_ts to now
	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Verify data is queryable — if GC prematurely dropped the segment,
	// this query would return 0 rows or fail.
	count := s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowNum), count,
		"import segment should not be GC'd — all rows should be queryable")

	// Wait a few seconds and query again to ensure stability
	time.Sleep(5 * time.Second)
	count = s.queryCountWithTs(ctx, collName, 0)
	s.Equal(int64(rowNum), count,
		"import segment should remain stable after GC cycles")
}
