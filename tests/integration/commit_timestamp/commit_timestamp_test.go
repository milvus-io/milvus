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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim = 128

	// commitTsOffset is how far into the future setCommitTimestamp stamps a
	// segment when a test needs reads and DML to land before the commit
	// timestamp. It has to cover bringing the coordinator back up, building
	// the index and loading the collection.
	commitTsOffset = 30 * time.Second
)

func TestCommitTimestampSuite(t *testing.T) {
	suite.Run(t, new(CommitTimestampSuite))
}

type CommitTimestampSuite struct {
	integration.MiniClusterSuite
}

// ─── Helpers ──────────────────────────────────────────────────────────────

// listDataSegments returns the persisted SegmentInfo of every flushed segment
// of the collection that holds rows, keyed by its etcd key.
//
// The record carries no binlog arrays -- datacoord strips them before
// serialization and stores them under the separate datacoord-meta/binlog
// prefix (V2), or resolves them from the LOON manifest (V3) -- so NumOfRows,
// which is on the record for both, is what says a segment holds data.
func (s *CommitTimestampSuite) listDataSegments(ctx context.Context, collectionID int64) map[string]*datapb.SegmentInfo {
	prefix := path.Join(s.Cluster.RootPath(), "meta/datacoord-meta/s",
		fmt.Sprintf("%d", collectionID)) + "/"

	resp, err := s.Cluster.EtcdCli.Get(ctx, prefix, clientv3.WithPrefix())
	s.Require().NoError(err, "failed to list segments from etcd")

	segments := make(map[string]*datapb.SegmentInfo)
	for _, kv := range resp.Kvs {
		seg := &datapb.SegmentInfo{}
		if err := proto.Unmarshal(kv.Value, seg); err != nil {
			continue
		}
		if seg.GetState() != commonpb.SegmentState_Flushed && seg.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		if seg.GetNumOfRows() == 0 {
			continue
		}
		segments[string(kv.Key)] = seg
	}
	return segments
}

// setCommitTimestamp stamps every flushed segment of the collection with a
// commit timestamp `offset` in the future and returns it along with the
// segments it modified. This simulates an import segment without going through
// the actual import pipeline.
//
// DataCoord serves segment meta from its in-memory SegmentsInfo, which is
// loaded from etcd at startup, so a write straight into etcd is invisible to a
// running coordinator: compaction plans, MVCC filtering and GC all read the
// in-memory copy. The write is therefore made while the coordinator is down.
// The commit timestamp is composed after the shutdown so that its offset only
// has to cover what the caller does next, not the shutdown itself.
func (s *CommitTimestampSuite) setCommitTimestamp(
	collectionID int64,
	offset time.Duration,
) (uint64, []int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Sort compaction runs asynchronously after flush and replaces the segment
	// it sorts with a copy whose CommitTimestamp is 0, so a stamp written
	// while it is in flight is silently dropped. Wait for it to land. It also
	// makes the segments selectable for mix compaction
	// (isMixCompactionSelectable), which TestCompaction_NormalizesCommitTs
	// depends on.
	s.Require().Eventually(func() bool {
		segments := s.listDataSegments(ctx, collectionID)
		if len(segments) == 0 {
			return false
		}
		for _, seg := range segments {
			if !seg.GetIsSorted() {
				return false
			}
		}
		return true
	}, time.Minute, time.Second, "flushed segments were never sorted")

	s.Cluster.DefaultMixCoord().Stop()
	commitTs := tsoutil.ComposeTSByTime(time.Now().Add(offset))

	var segmentIDs []int64
	for key, seg := range s.listDataSegments(ctx, collectionID) {
		s.Require().True(seg.GetIsSorted(),
			"segment %d became unsorted again after the wait", seg.GetID())

		mlog.Info(context.TODO(), "setCommitTimestamp: modifying segment",
			mlog.FieldSegmentID(seg.GetID()),
			mlog.Uint64("commitTs", commitTs))

		seg.CommitTimestamp = commitTs

		data, err := proto.Marshal(seg)
		s.Require().NoError(err)
		_, err = s.Cluster.EtcdCli.Put(ctx, key, string(data))
		s.Require().NoError(err)
		segmentIDs = append(segmentIDs, seg.GetID())
	}
	s.Require().NotEmpty(segmentIDs, "no flushed segments were modified")

	s.Cluster.AddMixCoord()

	return commitTs, segmentIDs
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
		// Customized, not the proto default Strong: the delegator replaces a
		// Strong read's guarantee timestamp with the WAL's current MVCC
		// timestamp (speedupGuranteeTS), which drops a timestamp that lies in
		// the future and serves the read at "now" instead of waiting for it.
		req.ConsistencyLevel = commonpb.ConsistencyLevel_Customized
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
	searchReq := integration.ConstructSearchRequestWithConsistencyLevel("", collName, "",
		integration.FloatVecField, schemapb.DataType_FloatVector, nil,
		metric.L2, params, 1, dim, topk, -1,
		false, commonpb.ConsistencyLevel_Customized)
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

	tBefore := tsoutil.ComposeTSByTime(time.Now())

	// Set commit_ts to a future time to test MVCC
	tCommit, _ := s.setCommitTimestamp(collectionID, commitTsOffset)
	tAfterCommit := tsoutil.AddPhysicalDurationOnTs(tCommit, 2*time.Second)

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
	s.setCommitTimestamp(collectionID, 0)

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

	tBefore := tsoutil.ComposeTSByTime(time.Now())
	tCommit, _ := s.setCommitTimestamp(collectionID, commitTsOffset)

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
	s.setCommitTimestamp(collectionID, 0)

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
	commitTs, _ := s.setCommitTimestamp(collectionID, commitTsOffset)

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
	s.setCommitTimestamp(collectionID, 0)

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
	commitTs, _ := s.setCommitTimestamp(collectionID, commitTsOffset)

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

	commitTs, modifiedSegIDs := s.setCommitTimestamp(collectionID, 0)
	s.Require().GreaterOrEqual(len(modifiedSegIDs), 2,
		"should have at least 2 segments to compact")

	// Trigger compaction
	compactResp, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: collectionID,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(compactResp.GetStatus()))

	// Wait for the mix compaction to replace the stamped segments, and take that
	// turnover as the proof it ran. Neither the returned compaction ID nor the
	// reported state can play that role: a trigger that selects no candidate
	// still allocates an ID, and GetCompactionState reports Completed with zero
	// plans both when no plan was ever generated and when a finished task has
	// been cleaned from the inspector, which happens within a second.
	stamped := typeutil.NewSet(modifiedSegIDs...)
	s.Require().Eventually(func() bool {
		segments, err := s.Cluster.ShowSegments(collName)
		if err != nil {
			return false
		}
		flushed := 0
		for _, seg := range segments {
			if seg.GetState() != commonpb.SegmentState_Flushed {
				continue
			}
			if stamped.Contain(seg.GetID()) {
				return false
			}
			flushed++
		}
		return flushed > 0
	}, 2*time.Minute, time.Second,
		"mix compaction never replaced the segments carrying a commit timestamp")

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
	s.setCommitTimestamp(collectionID, 0)

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
