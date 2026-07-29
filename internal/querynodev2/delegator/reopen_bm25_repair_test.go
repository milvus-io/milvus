// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delegator

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const reopenBM25RepairTestTimeout = 3 * time.Second

func (s *DelegatorDataSuite) addReadableSealedSegment(segmentID, rowCount int64) SegmentEntry {
	entry := SegmentEntry{
		NodeID:      1,
		SegmentID:   segmentID,
		PartitionID: 500,
		Version:     1,
		Level:       datapb.SegmentLevel_L1,
	}
	s.delegator.distribution.AddDistributions(entry)
	s.delegator.distribution.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         1,
		SealedInTarget:        []int64{segmentID},
		SealedSegmentRowCount: map[int64]int64{segmentID: rowCount},
	}, []int64{500})
	return entry
}

func (s *DelegatorDataSuite) bm25StatsData(rows uint32) []byte {
	stats := storage.NewBM25Stats()
	for i := uint32(0); i < rows; i++ {
		stats.Append(map[uint32]float32{i + 1: 1})
	}
	data, err := stats.Serialize()
	s.Require().NoError(err)
	return data
}

func (s *DelegatorDataSuite) reopenBM25Request(remotePath string) *querypb.LoadSegmentsRequest {
	return &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		LoadScope:    querypb.LoadScope_Reopen,
		Schema:       s.delegator.collection.Schema(),
		Version:      10,
		Infos: []*querypb.SegmentLoadInfo{{
			SegmentID:     100,
			PartitionID:   500,
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: s.vchannelName,
			DataVersion:   1,
			Bm25Logs:      bm25LogsForField(101, remotePath),
		}},
	}
}

func (s *DelegatorDataSuite) startReopenBM25Repair(req *querypb.LoadSegmentsRequest) *reopenBM25RepairEntry {
	entries := s.delegator.reserveReopenBM25Repairs(req)
	s.Require().Len(entries, 1)
	s.delegator.startReopenBM25Repair(entries[0])
	return entries[0]
}

func (s *DelegatorDataSuite) reopenBM25RepairFinished(entry *reopenBM25RepairEntry) bool {
	s.delegator.reopenBM25RepairMu.Lock()
	defer s.delegator.reopenBM25RepairMu.Unlock()
	return s.delegator.reopenBM25Repairs[entry.key] == nil
}

func (s *DelegatorDataSuite) bm25RowCount(fieldID int64) (int64, error) {
	oracle := s.getIDFOracleForTest()
	oracle.RLock()
	defer oracle.RUnlock()
	stats, err := oracle.current.GetStats(fieldID)
	if err != nil {
		return 0, err
	}
	return stats.NumRow(), nil
}

func (s *DelegatorDataSuite) TestLoadSegmentsReopenBM25FailureStartsRepair() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 2)

	remotePath := "bm25stats/reopen-repair/segment_100/field_101/transient"
	data := s.bm25StatsData(2)
	var attempts atomic.Int32
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("transient read failure")
		}
		return &bytesFileReader{bytes.NewReader(data)}, nil
	})
	s.loader.EXPECT().GetChunkManager().Return(cm)

	worker := &cluster.MockWorker{}
	worker.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).Return(nil)
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(worker, nil)

	err := s.delegator.LoadSegments(context.Background(), s.reopenBM25Request(remotePath))
	s.ErrorIs(err, merr.ErrCollectionRuntimeNotReady)
	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 2
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	s.Equal(int32(2), attempts.Load())
}

func (s *DelegatorDataSuite) TestReopenSecondSchemaGateFailureStillRepairsBM25() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/second-schema-gate/segment_100/field_101/0"
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(s.bm25StatsData(1))}, nil,
	).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	req := s.reopenBM25Request(remotePath)
	newSchema := typeutil.Clone(req.GetSchema())
	newSchema.Version++
	newSchema.Fields = append(newSchema.Fields, &schemapb.FieldSchema{
		FieldID:  103,
		Name:     "unrelated",
		DataType: schemapb.DataType_Bool,
	})
	worker := &cluster.MockWorker{}
	worker.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).RunAndReturn(
		func(context.Context, *querypb.LoadSegmentsRequest) error {
			s.delegator.schemaChangeMutex.Lock()
			s.delegator.publishDelegatorSchemaLocked(newSchema)
			s.delegator.schemaChangeMutex.Unlock()
			return nil
		})
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(worker, nil)

	err := s.delegator.LoadSegments(context.Background(), req)
	s.ErrorIs(err, merr.ErrCollectionSchemaVersionNotReady)
	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 1
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
}

func (s *DelegatorDataSuite) TestReopenPartialWorkerSuccessStillRepairsBM25() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/partial-worker/segment_100/field_101/0"
	data := s.bm25StatsData(1)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)
	workerErr := errors.New("injected worker failure")
	worker := &cluster.MockWorker{}
	worker.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).RunAndReturn(
		func(_ context.Context, req *querypb.LoadSegmentsRequest) error {
			if req.GetInfos()[0].GetSegmentID() == 101 {
				return workerErr
			}
			return nil
		})
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(worker, nil)

	req := s.reopenBM25Request(remotePath)
	failedInfo := typeutil.Clone(req.GetInfos()[0])
	failedInfo.SegmentID = 101
	failedInfo.Bm25Logs = bm25LogsForField(101, "bm25stats/partial-worker/segment_101/field_101/0")
	req.Infos = append(req.Infos, failedInfo)

	s.ErrorIs(s.delegator.LoadSegments(context.Background(), req), workerErr)
	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 1
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
}

func (s *DelegatorDataSuite) TestReopenBM25RepairWaitsForIDFTarget() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	oracle := s.getIDFOracleForTest()
	s.delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		SegmentID:   100,
		PartitionID: 500,
		Version:     1,
		Level:       datapb.SegmentLevel_L1,
	})
	s.delegator.distribution.SetIDFOracle(nil)
	s.delegator.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         1,
		SealedInTarget:        []int64{100},
		SealedSegmentRowCount: map[int64]int64{100: 1},
	}, []int64{500})
	s.delegator.distribution.SetIDFOracle(oracle)

	remotePath := "bm25stats/wait-idf-target/segment_100/field_101/0"
	data := s.bm25StatsData(1)
	var reads atomic.Int32
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
		reads.Add(1)
		return &bytesFileReader{bytes.NewReader(data)}, nil
	}).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	s.startReopenBM25Repair(s.reopenBM25Request(remotePath))
	time.Sleep(2 * reopenBM25RepairInitialBackoff)
	s.Zero(reads.Load())

	oracle.SetNext(s.delegator.distribution.current.Load())
	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 1
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	s.Equal(int32(1), reads.Load())
}

func (s *DelegatorDataSuite) TestReopenBM25RepairRejectsOldIncarnationAndRepairsReload() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	oldSegment := s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/reload-race/segment_100/field_101/0"
	data := s.bm25StatsData(1)
	readStarted := make(chan struct{})
	allowRead := make(chan struct{})
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
		close(readStarted)
		select {
		case <-allowRead:
			return &bytesFileReader{bytes.NewReader(data)}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Once()
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	oldEntry := s.startReopenBM25Repair(s.reopenBM25Request(remotePath))
	select {
	case <-readStarted:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("old BM25 repair did not start remote load")
	}

	s.delegator.distribution.RemoveDistributions([]SegmentEntry{oldSegment}, nil)
	s.delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		SegmentID:   100,
		PartitionID: 500,
		Version:     20,
		Level:       datapb.SegmentLevel_L1,
	})
	s.delegator.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         2,
		SealedInTarget:        []int64{100},
		SealedSegmentRowCount: map[int64]int64{100: 1},
	}, []int64{500})
	s.Require().Eventually(func() bool {
		return s.getIDFOracleForTest().TargetVersion() == 2
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	close(allowRead)
	s.Require().Eventually(func() bool {
		return s.reopenBM25RepairFinished(oldEntry)
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	rows, err := s.bm25RowCount(101)
	s.Require().NoError(err)
	s.Zero(rows)

	newReq := s.reopenBM25Request(remotePath)
	newReq.Version = 30
	newEntry := s.startReopenBM25Repair(newReq)
	s.NotEqual(oldEntry.key, newEntry.key)
	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 1
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
}

func (s *DelegatorDataSuite) TestLoadSegmentsReopenForegroundRejectsOldIncarnation() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	oldSegment := s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/foreground-reload-race/segment_100/field_101/0"
	readStarted := make(chan struct{})
	allowRead := make(chan struct{})
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
		close(readStarted)
		select {
		case <-allowRead:
			return &bytesFileReader{bytes.NewReader(s.bm25StatsData(1))}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	worker := &cluster.MockWorker{}
	worker.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).Return(nil)
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(worker, nil)

	loadDone := make(chan error, 1)
	go func() {
		loadDone <- s.delegator.LoadSegments(context.Background(), s.reopenBM25Request(remotePath))
	}()
	select {
	case <-readStarted:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("foreground BM25 load did not start")
	}

	s.delegator.distribution.RemoveDistributions([]SegmentEntry{oldSegment}, nil)
	s.delegator.distribution.AddDistributions(SegmentEntry{
		NodeID:      1,
		SegmentID:   100,
		PartitionID: 500,
		Version:     20,
		Level:       datapb.SegmentLevel_L1,
	})
	s.delegator.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         2,
		SealedInTarget:        []int64{100},
		SealedSegmentRowCount: map[int64]int64{100: 1},
	}, []int64{500})
	s.Require().Eventually(func() bool {
		return s.getIDFOracleForTest().TargetVersion() == 2
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	close(allowRead)

	select {
	case err := <-loadDone:
		s.NoError(err)
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("foreground BM25 load did not finish")
	}
	rows, err := s.bm25RowCount(101)
	s.Require().NoError(err)
	s.Zero(rows)
	s.delegator.reopenBM25RepairMu.Lock()
	s.Empty(s.delegator.reopenBM25Repairs)
	s.delegator.reopenBM25RepairMu.Unlock()
}

func (s *DelegatorDataSuite) TestReopenBM25RepairInstallFenceDoesNotWaitBehindSchemaWriter() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/schema-writer-fence/segment_100/field_101/0"
	readStarted := make(chan struct{})
	allowRead := make(chan struct{})
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
		close(readStarted)
		select {
		case <-allowRead:
			return &bytesFileReader{bytes.NewReader(s.bm25StatsData(1))}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	req := s.reopenBM25Request(remotePath)
	entry := s.delegator.newReopenBM25RepairEntry(req, req.GetInfos()[0])
	type loadResult struct {
		obsolete bool
		err      error
	}
	loadDone := make(chan loadResult, 1)
	go func() {
		obsolete, err := s.delegator.loadReopenBM25Stats(context.Background(), entry)
		loadDone <- loadResult{obsolete: obsolete, err: err}
	}()
	select {
	case <-readStarted:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("background BM25 load did not start")
	}

	s.delegator.schemaChangeMutex.RLock()
	leaseHeld := true
	defer func() {
		if leaseHeld {
			s.delegator.schemaChangeMutex.RUnlock()
		}
	}()
	writerDone := make(chan struct{})
	go func() {
		s.delegator.schemaChangeMutex.Lock()
		s.delegator.schemaChangeMutex.Unlock()
		close(writerDone)
	}()
	s.Require().Eventually(func() bool {
		if s.delegator.schemaChangeMutex.TryRLock() {
			s.delegator.schemaChangeMutex.RUnlock()
			return false
		}
		return true
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	close(allowRead)

	select {
	case result := <-loadDone:
		s.False(result.obsolete)
		s.ErrorIs(result.err, merr.ErrServiceNotReady)
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("BM25 install fence waited behind schema writer")
	}
	s.delegator.schemaChangeMutex.RUnlock()
	leaseHeld = false
	select {
	case <-writerDone:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("schema writer did not finish")
	}
}

func (s *DelegatorDataSuite) TestReopenBM25RepairRejectsChangedFieldSemantics() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/changed-analyzer/segment_100/field_101/0"
	data := s.bm25StatsData(1)
	readStarted := make(chan struct{})
	allowRead := make(chan struct{})
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
		close(readStarted)
		select {
		case <-allowRead:
			return &bytesFileReader{bytes.NewReader(data)}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	entry := s.startReopenBM25Repair(s.reopenBM25Request(remotePath))
	select {
	case <-readStarted:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("BM25 repair did not start remote load")
	}

	changedSchema := typeutil.Clone(entry.schema)
	changedSchema.Version++
	for _, field := range changedSchema.GetFields() {
		if field.GetFieldID() == 102 {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{Key: "analyzer_params", Value: `{"type":"english"}`})
		}
	}
	s.delegator.schemaChangeMutex.Lock()
	s.delegator.publishDelegatorSchemaLocked(changedSchema)
	s.delegator.schemaChangeMutex.Unlock()
	close(allowRead)

	s.Require().Eventually(func() bool {
		return s.reopenBM25RepairFinished(entry)
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	segmentStats, ok := s.getIDFOracleForTest().sealed.Get(100)
	if ok {
		s.False(segmentStats.HasField(101))
	}
}

func (s *DelegatorDataSuite) TestReopenBM25RepairDeduplicatesQueryCoordRetry() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)

	req := s.reopenBM25Request("bm25stats/deduplicate/segment_100/field_101/0")
	req.Schema = typeutil.Clone(req.GetSchema())
	req.Schema.Fields = append(req.Schema.Fields,
		&schemapb.FieldSchema{FieldID: 103, Name: "vector_2", DataType: schemapb.DataType_SparseFloatVector},
		&schemapb.FieldSchema{FieldID: 104, Name: "text_2", DataType: schemapb.DataType_VarChar},
	)
	req.Schema.Functions = append(req.Schema.Functions, &schemapb.FunctionSchema{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{104},
		OutputFieldIds: []int64{103},
	})
	first := s.delegator.newReopenBM25RepairEntry(req, req.GetInfos()[0])
	s.Same(first, s.delegator.reserveReopenBM25Repair(first))

	retryReq := typeutil.Clone(req)
	retryReq.Version++
	retried := s.delegator.newReopenBM25RepairEntry(retryReq, retryReq.GetInfos()[0])
	s.Same(first, s.delegator.reserveReopenBM25Repair(retried))
	s.delegator.reopenBM25RepairMu.Lock()
	s.Same(first, s.delegator.reopenBM25Repairs[first.key])
	s.delegator.reopenBM25RepairMu.Unlock()
	disjointReq := typeutil.Clone(req)
	disjointReq.Infos[0].Bm25Logs = bm25LogsForField(103, "bm25stats/deduplicate/segment_100/field_103/0")
	disjoint := s.delegator.newReopenBM25RepairEntry(disjointReq, disjointReq.GetInfos()[0])
	s.Same(disjoint, s.delegator.reserveReopenBM25Repair(disjoint))
	s.NotEqual(first.key, disjoint.key)

	newPayloadReq := typeutil.Clone(req)
	newPayloadReq.Infos[0].DataVersion++
	newPayloadReq.Infos[0].ManifestPath = `{"ver":2,"base_path":"files/segment_100"}`
	newPayload := s.delegator.newReopenBM25RepairEntry(newPayloadReq, newPayloadReq.GetInfos()[0])
	s.Same(newPayload, s.delegator.reserveReopenBM25Repair(newPayload))
	s.NotEqual(first.key, newPayload.key)
	s.delegator.finishReopenBM25Repair(first)
	s.delegator.finishReopenBM25Repair(disjoint)
	s.delegator.finishReopenBM25Repair(newPayload)
}

func (s *DelegatorDataSuite) TestReopenBM25RepairDoesNotDeduplicateNewLegacyPath() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)

	oldPath := "bm25stats/path-refresh/segment_100/field_101/old"
	newPath := "bm25stats/path-refresh/segment_100/field_101/new"
	oldRead := make(chan struct{})
	var oldReadOnce sync.Once
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, oldPath).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
		oldReadOnce.Do(func() { close(oldRead) })
		return nil, errors.New("stale bm25 stats path")
	}).Maybe()
	cm.EXPECT().Reader(mock.Anything, newPath).Return(
		&bytesFileReader{bytes.NewReader(s.bm25StatsData(1))}, nil,
	).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)

	oldEntry := s.startReopenBM25Repair(s.reopenBM25Request(oldPath))
	select {
	case <-oldRead:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("old BM25 path was not attempted")
	}

	newEntry := s.startReopenBM25Repair(s.reopenBM25Request(newPath))
	s.NotEqual(oldEntry.key, newEntry.key)
	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 1
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
	s.Require().Eventually(func() bool {
		return s.reopenBM25RepairFinished(oldEntry) && s.reopenBM25RepairFinished(newEntry)
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
}

func (s *DelegatorDataSuite) TestBM25SchemaDefinitionIgnoresLoadOnlyFieldParams() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()

	walSchema := s.delegator.collection.Schema()
	requestSchema := typeutil.Clone(walSchema)
	for _, field := range requestSchema.GetFields() {
		if field.GetFieldID() == 102 {
			field.TypeParams = append(field.TypeParams,
				&commonpb.KeyValuePair{Key: "mmap.enabled", Value: "true"},
				&commonpb.KeyValuePair{Key: "warmup", Value: "sync"},
			)
		}
	}
	s.True(sameBM25SchemaDefinition(requestSchema, walSchema, 101))
}

func (s *DelegatorDataSuite) TestReopenBM25SchemaFenceKeepsCompatibleFields() {
	s.genCollectionWithFunction()
	defer s.delegator.Close()

	expected := typeutil.Clone(s.delegator.collection.Schema())
	expected.Fields = append(expected.Fields,
		&schemapb.FieldSchema{FieldID: 103, Name: "vector_2", DataType: schemapb.DataType_SparseFloatVector},
		&schemapb.FieldSchema{FieldID: 104, Name: "text_2", DataType: schemapb.DataType_VarChar},
	)
	expected.Functions = append(expected.Functions, &schemapb.FunctionSchema{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{104},
		OutputFieldIds: []int64{103},
	})
	current := typeutil.Clone(expected)
	current.Version++
	current.Functions = current.Functions[:1]
	s.delegator.schemaChangeMutex.Lock()
	s.delegator.publishDelegatorSchemaLocked(current)
	s.delegator.schemaChangeMutex.Unlock()

	req := s.reopenBM25Request("bm25stats/schema-filter/segment_100/field_101/0")
	req.Schema = expected
	entry := s.delegator.newReopenBM25RepairEntry(req, req.GetInfos()[0])
	s.delegator.schemaChangeMutex.RLock()
	_, compatible, obsolete, err := s.delegator.validateReopenBM25SchemaLocked(entry, map[int64][]string{
		101: {"field_101"},
		103: {"field_103"},
	})
	s.delegator.schemaChangeMutex.RUnlock()
	s.NoError(err)
	s.False(obsolete)
	s.Equal(map[int64][]string{101: {"field_101"}}, compatible)
}

func (s *DelegatorDataSuite) TestReopenBM25RepairSurvivesWatchContextCancellation() {
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	s.genCollectionWithFunctionContext(watchCtx)
	defer s.delegator.Close()
	s.addReadableSealedSegment(100, 1)
	cancelWatch()

	remotePath := "bm25stats/watch-context/segment_100/field_101/0"
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(s.bm25StatsData(1))}, nil,
	).Once()
	s.loader.EXPECT().GetChunkManager().Return(cm)
	s.startReopenBM25Repair(s.reopenBM25Request(remotePath))

	s.Require().Eventually(func() bool {
		rows, err := s.bm25RowCount(101)
		return err == nil && rows == 1
	}, reopenBM25RepairTestTimeout, 10*time.Millisecond)
}

func (s *DelegatorDataSuite) TestReopenBM25RepairCloseCancelsAttempt() {
	s.genCollectionWithFunction()
	s.addReadableSealedSegment(100, 1)

	remotePath := "bm25stats/close/segment_100/field_101/0"
	repairStarted := make(chan struct{})
	var once sync.Once
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
		once.Do(func() { close(repairStarted) })
		<-ctx.Done()
		return nil, ctx.Err()
	})
	s.loader.EXPECT().GetChunkManager().Return(cm)
	s.startReopenBM25Repair(s.reopenBM25Request(remotePath))

	select {
	case <-repairStarted:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("background BM25 repair did not start")
	}
	closed := make(chan struct{})
	go func() {
		s.delegator.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(reopenBM25RepairTestTimeout):
		s.FailNow("delegator close did not cancel BM25 repair")
	}
}
