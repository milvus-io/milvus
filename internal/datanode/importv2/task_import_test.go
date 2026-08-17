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
	"io"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ImportTaskDeleteModeSuite covers the delete-mode write path of ImportTask.importFile:
// a delete-key file is read, projected to primary keys, and written as delete records
// into the job's L0 request segment.
type ImportTaskDeleteModeSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
	channel      string
	rowCount     int
	importTs     uint64

	schema  *schemapb.CollectionSchema
	pks     []int64
	manager TaskManager
	syncMgr *syncmgr.MockSyncManager
}

func (s *ImportTaskDeleteModeSuite) SetupSuite() {
	paramtable.Init()
}

func (s *ImportTaskDeleteModeSuite) SetupTest() {
	s.collectionID = 1
	s.partitionID = 2
	s.segmentID = 3
	s.channel = "ch-0"
	s.rowCount = 5
	// A distinctive, nonzero value: if the write path ever stamps deletes with anything
	// else (zero value, wall-clock time, a commit timestamp), this value discriminates it.
	s.importTs = 123456789

	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}

	s.pks = make([]int64, s.rowCount)
	for i := 0; i < s.rowCount; i++ {
		s.pks[i] = int64(i + 1)
	}

	s.manager = NewTaskManager()
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
}

// newInputChunkManager returns a ChunkManager mock that serves a delete-key JSON file
// containing s.pks as the "pk" column, the pattern used by the neighboring preimport tests.
func (s *ImportTaskDeleteModeSuite) newInputChunkManager() *mocks.ChunkManager {
	content := &deleteKeyContent{Rows: make([]deleteKeyRow, 0, s.rowCount)}
	for _, pk := range s.pks {
		content.Rows = append(content.Rows, deleteKeyRow{PK: pk, Extra: "ignored"})
	}
	bytes, err := json.Marshal(content)
	s.Require().NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	return cm
}

func (s *ImportTaskDeleteModeSuite) newImportRequest(segments []*datapb.ImportRequestSegment) *datapb.ImportRequest {
	return &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    []string{s.channel},
		Schema:       s.schema,
		Files:        []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Delete"},
		},
		Ts: s.importTs,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.rowCount) + 100,
		},
		RequestSegments: segments,
	}
}

// TestDeleteMode_WritesL0DeleteOnly drives a full ImportTask.Execute() over a delete-key
// file and checks the resulting sync: it targets the request's L0 segment, carries no
// insert data, holds one delete record per input row, and every delete record is stamped
// with the import request's own timetick (req.Ts) — never a later commit timestamp.
func (s *ImportTaskDeleteModeSuite) TestDeleteMode_WritesL0DeleteOnly() {
	cm := s.newInputChunkManager()
	var capturedBlob []byte
	cm.EXPECT().RootPath().Return("mock-rootpath")
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, content []byte) error {
			capturedBlob = append([]byte(nil), content...)
			return nil
		})

	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, task syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			syncTask := task.(*syncmgr.SyncTask)
			syncTask.WithChunkManager(chunkManager)
			err := syncTask.Run(ctx)
			s.Require().NoError(err)
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		})

	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	futures := task.Execute()
	err := conc.AwaitAll(futures...)
	s.Require().NoError(err)

	s.NotEqual(datapb.ImportTaskStateV2_Failed, s.manager.Get(task.GetTaskID()).GetState())

	importTask := s.manager.Get(task.GetTaskID()).(*ImportTask)
	segInfos := importTask.GetSegmentsInfo()
	s.Require().Len(segInfos, 1)
	info := segInfos[0]
	s.Equal(s.segmentID, info.GetSegmentID())
	s.Empty(info.GetBinlogs())
	s.Empty(info.GetStatslogs())
	s.Require().Len(info.GetDeltalogs(), 1)
	s.Require().Len(info.GetDeltalogs()[0].GetBinlogs(), 1)
	s.EqualValues(s.rowCount, info.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())

	// Decode the actual bytes written to the delta log: this is what segcore reads,
	// so it is the only conclusive check of the per-record timestamp invariant.
	s.Require().NotEmpty(capturedBlob)
	reader, err := storage.CreateDeltalogReader([]*storage.Blob{{Value: capturedBlob}})
	s.Require().NoError(err)
	defer reader.Close()

	logs := make([]*storage.DeleteLog, 0, s.rowCount)
	for {
		log, err := reader.NextValue()
		if err != nil {
			break
		}
		if log != nil {
			logs = append(logs, *log)
		}
	}
	s.Require().Len(logs, s.rowCount)
	gotPks := make([]int64, 0, s.rowCount)
	for _, l := range logs {
		s.EqualValues(s.importTs, l.Ts)
		gotPks = append(gotPks, l.Pk.GetValue().(int64))
	}
	s.ElementsMatch(s.pks, gotPks)
}

// TestDeleteMode_RequiresL0RequestSegment checks that delete-mode picks a request segment
// by L0 level specifically: with only an L1 segment offered for the channel/partition, the
// task must fail rather than silently writing into the L1 segment.
func (s *ImportTaskDeleteModeSuite) TestDeleteMode_RequiresL0RequestSegment() {
	cm := s.newInputChunkManager()
	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L1},
	}
	req := s.newImportRequest(segments)

	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	futures := task.Execute()
	err := conc.AwaitAll(futures...)
	s.Error(err)

	got := s.manager.Get(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Failed, got.GetState())
	s.Contains(got.GetReason(), "L0")
}

// TestDeleteMode_ProcessesAllReadBatches checks that importFile keeps reading and syncing
// every batch a reader returns, rather than stopping after the first one.
func (s *ImportTaskDeleteModeSuite) TestDeleteMode_ProcessesAllReadBatches() {
	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)

	task := NewImportTask(req, s.manager, s.syncMgr, mocks.NewChunkManager(s.T())).(*ImportTask)

	batch1 := &storage.InsertData{Data: map[int64]storage.FieldData{100: &storage.Int64FieldData{Data: []int64{1, 2}}}}
	batch2 := &storage.InsertData{Data: map[int64]storage.FieldData{100: &storage.Int64FieldData{Data: []int64{3, 4, 5}}}}

	reader := importutilv2.NewMockReader(s.T())
	reader.EXPECT().Read().Return(batch1, nil).Once()
	reader.EXPECT().Read().Return(batch2, nil).Once()
	reader.EXPECT().Read().Return(nil, io.EOF).Once()

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, syncTask syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		}).Times(2)

	err := task.importFile(reader)
	s.Require().NoError(err)
}

func TestImportTaskDeleteMode(t *testing.T) {
	suite.Run(t, new(ImportTaskDeleteModeSuite))
}

// upsertRow is a full insert-file row: primary key plus vector, the shape an
// upsert-mode import file carries (unlike a delete-key file, which carries the
// primary key only).
type upsertRow struct {
	PK  int64     `json:"pk"`
	Vec []float32 `json:"vec"`
}

type upsertContent struct {
	Rows []upsertRow `json:"rows,omitempty"`
}

// ImportTaskUpsertModeSuite covers the upsert-mode write path of ImportTask.importFile:
// a full-row file is read once and produces two writes per batch, insert data into the
// job's L1 request segment and companion delete records into its L0 request segment,
// with the companion deletes stamped with the import request's own timetick (req.Ts) so
// they remove pre-existing rows without touching the rows this same job just wrote.
type ImportTaskUpsertModeSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	l1SegmentID  int64
	l0SegmentID  int64
	channel      string
	rowCount     int
	importTs     uint64

	schema  *schemapb.CollectionSchema
	pks     []int64
	vecs    [][]float32
	manager TaskManager
	syncMgr *syncmgr.MockSyncManager
}

func (s *ImportTaskUpsertModeSuite) SetupSuite() {
	paramtable.Init()
}

func (s *ImportTaskUpsertModeSuite) SetupTest() {
	s.collectionID = 1
	s.partitionID = 2
	s.l1SegmentID = 50
	s.l0SegmentID = 51
	s.channel = "ch-0"
	s.rowCount = 5
	// A distinctive, nonzero value: if the companion delete is ever stamped with
	// anything else (zero value, wall-clock time, a commit timestamp), this value
	// discriminates it.
	s.importTs = 123456789

	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}

	s.pks = make([]int64, s.rowCount)
	s.vecs = make([][]float32, s.rowCount)
	for i := 0; i < s.rowCount; i++ {
		s.pks[i] = int64(i + 1)
		s.vecs[i] = []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4}
	}

	s.manager = NewTaskManager()
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
}

// newInputChunkManager returns a ChunkManager mock that serves a full insert-file JSON
// containing s.pks and s.vecs, the pattern used by the neighboring append-mode tests.
func (s *ImportTaskUpsertModeSuite) newInputChunkManager() *mocks.ChunkManager {
	content := &upsertContent{Rows: make([]upsertRow, 0, s.rowCount)}
	for i := 0; i < s.rowCount; i++ {
		content.Rows = append(content.Rows, upsertRow{PK: s.pks[i], Vec: s.vecs[i]})
	}
	bytes, err := json.Marshal(content)
	s.Require().NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	return cm
}

func (s *ImportTaskUpsertModeSuite) newImportRequest(segments []*datapb.ImportRequestSegment) *datapb.ImportRequest {
	return &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    []string{s.channel},
		Schema:       s.schema,
		Files:        []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Upsert"},
		},
		Ts: s.importTs,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.rowCount) + 100,
		},
		RequestSegments: segments,
	}
}

// TestUpsertMode_WritesL1InsertAndL0CompanionDelete drives a full ImportTask.Execute()
// over an insert file under write_mode=Upsert and checks the resulting sync: row data
// lands on the L1 request segment, one companion delete record per row lands on the L0
// request segment, every companion delete carries the import request's own timetick
// (req.Ts) rather than a later commit timestamp, and the companion delete primary keys
// are the same keys as the rows written.
func (s *ImportTaskUpsertModeSuite) TestUpsertMode_WritesL1InsertAndL0CompanionDelete() {
	cm := s.newInputChunkManager()
	var capturedDeltaBlob []byte
	cm.EXPECT().RootPath().Return("mock-rootpath")
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, key string, content []byte) error {
			if strings.Contains(key, "delta_log") {
				capturedDeltaBlob = append([]byte(nil), content...)
			}
			return nil
		})

	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.l1SegmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L1},
		{SegmentID: s.l0SegmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, task syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			syncTask := task.(*syncmgr.SyncTask)
			syncTask.WithChunkManager(chunkManager)
			err := syncTask.Run(ctx)
			s.Require().NoError(err)
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		})

	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	futures := task.Execute()
	err := conc.AwaitAll(futures...)
	s.Require().NoError(err)

	s.NotEqual(datapb.ImportTaskStateV2_Failed, s.manager.Get(task.GetTaskID()).GetState())

	importTask := s.manager.Get(task.GetTaskID()).(*ImportTask)
	segInfos := importTask.GetSegmentsInfo()
	s.Require().Len(segInfos, 2)

	var l1Info, l0Info *datapb.ImportSegmentInfo
	for _, info := range segInfos {
		switch info.GetSegmentID() {
		case s.l1SegmentID:
			l1Info = info
		case s.l0SegmentID:
			l0Info = info
		}
	}
	s.Require().NotNil(l1Info, "L1 request segment must receive a sync")
	s.Require().NotNil(l0Info, "L0 request segment must receive a sync")

	// L1 receives the row data.
	s.NotEmpty(l1Info.GetBinlogs())
	s.Empty(l1Info.GetDeltalogs())
	s.EqualValues(s.rowCount, l1Info.GetImportedRows())

	// L0 receives the companion deletes, one per row.
	s.Empty(l0Info.GetBinlogs())
	s.Require().Len(l0Info.GetDeltalogs(), 1)
	s.Require().Len(l0Info.GetDeltalogs()[0].GetBinlogs(), 1)
	s.EqualValues(s.rowCount, l0Info.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())

	// Decode the actual bytes written to the delta log: this is what segcore reads,
	// so it is the only conclusive check of the per-record timestamp invariant.
	s.Require().NotEmpty(capturedDeltaBlob)
	reader, err := storage.CreateDeltalogReader([]*storage.Blob{{Value: capturedDeltaBlob}})
	s.Require().NoError(err)
	defer reader.Close()

	logs := make([]*storage.DeleteLog, 0, s.rowCount)
	for {
		log, err := reader.NextValue()
		if err != nil {
			break
		}
		if log != nil {
			logs = append(logs, *log)
		}
	}
	s.Require().Len(logs, s.rowCount)
	gotPks := make([]int64, 0, s.rowCount)
	for _, l := range logs {
		s.EqualValues(s.importTs, l.Ts)
		gotPks = append(gotPks, l.Pk.GetValue().(int64))
	}
	s.ElementsMatch(s.pks, gotPks)
}

// TestUpsertMode_ProcessesAllReadBatches checks that importFile keeps reading and syncing
// every batch under write_mode=Upsert, rather than stopping after the first one: each batch
// produces both an insert sync and a companion-delete sync.
func (s *ImportTaskUpsertModeSuite) TestUpsertMode_ProcessesAllReadBatches() {
	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.l1SegmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L1},
		{SegmentID: s.l0SegmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)
	task := NewImportTask(req, s.manager, s.syncMgr, mocks.NewChunkManager(s.T())).(*ImportTask)

	batch1, err := testutil.CreateInsertData(s.schema, 2)
	s.Require().NoError(err)
	batch2, err := testutil.CreateInsertData(s.schema, 3)
	s.Require().NoError(err)

	reader := importutilv2.NewMockReader(s.T())
	reader.EXPECT().Read().Return(batch1, nil).Once()
	reader.EXPECT().Read().Return(batch2, nil).Once()
	reader.EXPECT().Read().Return(nil, io.EOF).Once()

	// 2 batches x (1 insert sync + 1 companion-delete sync).
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, syncTask syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		}).Times(4)

	err = task.importFile(reader)
	s.Require().NoError(err)
}

func TestImportTaskUpsertMode(t *testing.T) {
	suite.Run(t, new(ImportTaskUpsertModeSuite))
}

// upsertPartitionRow is a full insert-file row for a partition-key collection.
type upsertPartitionRow struct {
	PK      int64     `json:"pk"`
	PartKey int64     `json:"part_key"`
	Vec     []float32 `json:"vec"`
}

type upsertPartitionContent struct {
	Rows []upsertPartitionRow `json:"rows,omitempty"`
}

// ImportTaskUpsertPartitionKeySuite covers the partition routing of upsert-mode companion
// deletes on a partition-key collection: an upsert batch spans multiple partitions, and each
// row's companion delete must land in the L0 segment for that row's own partition rather than
// all being routed to the request's first partition.
type ImportTaskUpsertPartitionKeySuite struct {
	suite.Suite

	collectionID int64
	channel      string
	importTs     uint64

	partitionAID int64
	partitionBID int64
	l1SegA       int64
	l0SegA       int64
	l1SegB       int64
	l0SegB       int64

	schema   *schemapb.CollectionSchema
	partKeyA int64
	partKeyB int64
	pksInA   []int64
	pksInB   []int64

	manager TaskManager
	syncMgr *syncmgr.MockSyncManager
}

func (s *ImportTaskUpsertPartitionKeySuite) SetupSuite() {
	paramtable.Init()
}

func (s *ImportTaskUpsertPartitionKeySuite) SetupTest() {
	s.collectionID = 1
	s.channel = "ch-0"
	s.importTs = 123456789

	s.partitionAID = 10
	s.partitionBID = 20
	s.l1SegA = 9001
	s.l0SegA = 9002
	s.l1SegB = 9003
	s.l0SegB = 9004

	s.partKeyA, s.partKeyB = s.findPartitionKeys()

	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:        101,
				Name:           "part_key",
				IsPartitionKey: true,
				DataType:       schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}

	s.pksInA = []int64{1, 2, 3}
	s.pksInB = []int64{4, 5}

	s.manager = NewTaskManager()
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
}

// findPartitionKeys returns two partition-key values that land in different buckets under
// hashByPartition for a 2-partition collection, the same hash (typeutil.Hash32Int64) that
// HashData and HashDeleteDataForUpsert use.
func (s *ImportTaskUpsertPartitionKeySuite) findPartitionKeys() (int64, int64) {
	var key0, key1 int64
	var found0, found1 bool
	for i := int64(0); i < 1000 && (!found0 || !found1); i++ {
		h, err := typeutil.Hash32Int64(i)
		s.Require().NoError(err)
		if int64(h)%2 == 0 && !found0 {
			key0, found0 = i, true
		} else if int64(h)%2 == 1 && !found1 {
			key1, found1 = i, true
		}
	}
	s.Require().True(found0 && found1, "could not find partition-key values landing in both buckets")
	return key0, key1
}

// newInputChunkManager returns a ChunkManager mock that serves a full insert-file JSON
// containing s.pksInA rows tagged with s.partKeyA and s.pksInB rows tagged with s.partKeyB.
func (s *ImportTaskUpsertPartitionKeySuite) newInputChunkManager() *mocks.ChunkManager {
	vec := func(pk int64) []float32 {
		return []float32{float32(pk) + 0.1, float32(pk) + 0.2, float32(pk) + 0.3, float32(pk) + 0.4}
	}
	content := &upsertPartitionContent{Rows: make([]upsertPartitionRow, 0, len(s.pksInA)+len(s.pksInB))}
	for _, pk := range s.pksInA {
		content.Rows = append(content.Rows, upsertPartitionRow{PK: pk, PartKey: s.partKeyA, Vec: vec(pk)})
	}
	for _, pk := range s.pksInB {
		content.Rows = append(content.Rows, upsertPartitionRow{PK: pk, PartKey: s.partKeyB, Vec: vec(pk)})
	}
	bytes, err := json.Marshal(content)
	s.Require().NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	return cm
}

func (s *ImportTaskUpsertPartitionKeySuite) newImportRequest(segments []*datapb.ImportRequestSegment) *datapb.ImportRequest {
	return &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionAID, s.partitionBID},
		Vchannels:    []string{s.channel},
		Schema:       s.schema,
		Files:        []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Upsert"},
		},
		Ts: s.importTs,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(len(s.pksInA)+len(s.pksInB)) + 100,
		},
		RequestSegments: segments,
	}
}

// TestUpsertMode_RoutesCompanionDeletesByPartition drives a full ImportTask.Execute() over
// an insert file spanning two partitions under write_mode=Upsert, and checks that each row's
// companion delete lands in the L0 segment for that row's own partition: partition A's L0
// segment carries only partition A's primary keys, and partition B's only partition B's.
func (s *ImportTaskUpsertPartitionKeySuite) TestUpsertMode_RoutesCompanionDeletesByPartition() {
	cm := s.newInputChunkManager()
	capturedWrites := make(map[string][]byte)
	cm.EXPECT().RootPath().Return("mock-rootpath")
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, key string, content []byte) error {
			capturedWrites[key] = append([]byte(nil), content...)
			return nil
		})

	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.l1SegA, PartitionID: s.partitionAID, Vchannel: s.channel, Level: datapb.SegmentLevel_L1},
		{SegmentID: s.l0SegA, PartitionID: s.partitionAID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
		{SegmentID: s.l1SegB, PartitionID: s.partitionBID, Vchannel: s.channel, Level: datapb.SegmentLevel_L1},
		{SegmentID: s.l0SegB, PartitionID: s.partitionBID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, task syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			syncTask := task.(*syncmgr.SyncTask)
			syncTask.WithChunkManager(chunkManager)
			err := syncTask.Run(ctx)
			s.Require().NoError(err)
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		})

	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	futures := task.Execute()
	err := conc.AwaitAll(futures...)
	s.Require().NoError(err)
	s.NotEqual(datapb.ImportTaskStateV2_Failed, s.manager.Get(task.GetTaskID()).GetState())

	importTask := s.manager.Get(task.GetTaskID()).(*ImportTask)
	segInfos := importTask.GetSegmentsInfo()
	s.Require().Len(segInfos, 4)

	infoByID := lo.SliceToMap(segInfos, func(info *datapb.ImportSegmentInfo) (int64, *datapb.ImportSegmentInfo) {
		return info.GetSegmentID(), info
	})

	l1A, ok := infoByID[s.l1SegA]
	s.Require().True(ok, "L1 segment for partition A must receive a sync")
	l1B, ok := infoByID[s.l1SegB]
	s.Require().True(ok, "L1 segment for partition B must receive a sync")
	l0A, ok := infoByID[s.l0SegA]
	s.Require().True(ok, "L0 segment for partition A must receive a sync")
	l0B, ok := infoByID[s.l0SegB]
	s.Require().True(ok, "L0 segment for partition B must receive a sync")

	s.EqualValues(len(s.pksInA), l1A.GetImportedRows())
	s.EqualValues(len(s.pksInB), l1B.GetImportedRows())

	s.Require().Len(l0A.GetDeltalogs(), 1)
	s.Require().Len(l0A.GetDeltalogs()[0].GetBinlogs(), 1)
	s.EqualValues(len(s.pksInA), l0A.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())

	s.Require().Len(l0B.GetDeltalogs(), 1)
	s.Require().Len(l0B.GetDeltalogs()[0].GetBinlogs(), 1)
	s.EqualValues(len(s.pksInB), l0B.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())

	s.ElementsMatch(s.pksInA, s.decodeDeltaPks(capturedWrites, s.l0SegA, len(s.pksInA)))
	s.ElementsMatch(s.pksInB, s.decodeDeltaPks(capturedWrites, s.l0SegB, len(s.pksInB)))
}

// decodeDeltaPks finds the delta-log blob written for segmentID and decodes its primary keys,
// asserting each record's timestamp equals s.importTs along the way.
func (s *ImportTaskUpsertPartitionKeySuite) decodeDeltaPks(writes map[string][]byte, segmentID int64, expectCount int) []int64 {
	var blob []byte
	for key, content := range writes {
		if strings.Contains(key, "delta_log") && strings.Contains(key, fmt.Sprintf("/%d/", segmentID)) {
			blob = content
			break
		}
	}
	s.Require().NotEmptyf(blob, "no delta log captured for segment %d", segmentID)

	reader, err := storage.CreateDeltalogReader([]*storage.Blob{{Value: blob}})
	s.Require().NoError(err)
	defer reader.Close()

	logs := make([]*storage.DeleteLog, 0, expectCount)
	for {
		log, err := reader.NextValue()
		if err != nil {
			break
		}
		if log != nil {
			logs = append(logs, *log)
		}
	}
	s.Require().Len(logs, expectCount)
	pks := make([]int64, 0, expectCount)
	for _, l := range logs {
		s.EqualValues(s.importTs, l.Ts)
		pks = append(pks, l.Pk.GetValue().(int64))
	}
	return pks
}

func TestImportTaskUpsertPartitionKey(t *testing.T) {
	suite.Run(t, new(ImportTaskUpsertPartitionKeySuite))
}

// TestInsertRequestSegments_ExcludesL0 pins insertRequestSegments' candidate set directly,
// without going through PickSegment's random choice among candidates: L0 must never be a
// candidate for row data, while L1 and legacy (zero-value level, pre-dating this feature)
// segments remain candidates.
func TestInsertRequestSegments_ExcludesL0(t *testing.T) {
	segments := []*datapb.ImportRequestSegment{
		{SegmentID: 1, PartitionID: 10, Vchannel: "ch0", Level: datapb.SegmentLevel_L1},
		{SegmentID: 2, PartitionID: 10, Vchannel: "ch0", Level: datapb.SegmentLevel_L0},
		{SegmentID: 3, PartitionID: 10, Vchannel: "ch0"},
	}
	got := insertRequestSegments(segments)
	gotIDs := lo.Map(got, func(info *datapb.ImportRequestSegment, _ int) int64 { return info.GetSegmentID() })
	assert.ElementsMatch(t, []int64{1, 3}, gotIDs)
}
