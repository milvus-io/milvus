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

package compactor

// Real write + real read-back direct tests for compactionSegmentStorageFields
// and its public wrapper newCompactionSegmentRecordReader. NO mock of the tested
// logic: segments are written by the real writers to real storage and read back
// through the production read path. Both tests pin the dispatch invariant
// (manifest present -> manifest field set; manifest empty -> binlog field set) so
// a branch flip goes RED — the gap the 100%-statement-coverage-but-no-oracle
// state left open.

import (
	"context"
	"io"
	"math"

	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TestCompactionSegmentStorageFields_V3ManifestBranch_RealReadWrite — the manifest
// branch. A real StorageV3 segment is written to disk (MultiSegmentWriter); the
// method's field set must equal the committed manifest read back from disk, and
// the public reader must physically read the rows back. Dispatch invariant: with a
// manifest present, FieldBinlogs must be ignored — a flip to the binlog branch
// would surface a field that only lives in FieldBinlogs.
func (s *BumpSchemaVersionCompactionTaskSuite) TestCompactionSegmentStorageFields_V3ManifestBranch_RealReadWrite() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
	s.finishBumpSchemaVersionSegment()

	seg := s.task.plan.SegmentBinlogs[0]
	cfg := s.task.compactionParams.StorageConfig
	s.Require().NotEmpty(seg.GetManifest(), "V3 segment must carry a real manifest")

	// Method under test — no mock.
	got, err := compactionSegmentStorageFields(seg, cfg)
	s.Require().NoError(err)

	// Independent storage truth: read the real manifest file back off disk.
	truth, err := packed.GetManifestFieldIDs(seg.GetManifest(), cfg)
	s.Require().NoError(err)
	s.Equal(truth, got, "V3 branch must return exactly the committed-manifest field set")
	for _, fid := range []int64{common.RowIDField, common.TimeStampField, 100, 101} {
		s.Contains(got, fid, "physical field %d must be present", fid)
	}

	// Public wrapper end-to-end: open + physically read every row back off disk.
	reader, existing, err := newCompactionSegmentRecordReader(context.Background(), seg,
		&schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}, cfg,
		storage.WithVersion(seg.GetStorageVersion()),
		storage.WithDownloader(s.task.chunkManager.MultiRead),
		storage.WithStorageConfig(cfg),
	)
	s.Require().NoError(err)
	defer reader.Close()
	s.Equal(got, existing, "wrapper must surface the same field set the method computed")
	rows := 0
	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		s.Require().NoError(err)
		rows += rec.Len()
	}
	s.Equal(3, rows, "3 rows physically read back from the V3 segment")

	// Dispatch invariant (mutation-tight): same real manifest, but FieldBinlogs
	// carry a bogus field 777. The manifest branch must ignore FieldBinlogs; a flip
	// to the binlog branch would surface 777. Still a real manifest read.
	seg2 := proto.Clone(seg).(*datapb.CompactionSegmentBinlogs)
	seg2.FieldBinlogs = []*datapb.FieldBinlog{{FieldID: 777}}
	got2, err := compactionSegmentStorageFields(seg2, cfg)
	s.Require().NoError(err)
	s.NotContains(got2, int64(777), "manifest branch must not consult FieldBinlogs (dispatch invariant)")
	s.Equal(truth, got2, "manifest branch stays anchored to the committed manifest")
}

// TestCompactionSegmentStorageFields_V2BinlogBranch_RealReadWrite — the binlog
// branch. A real StorageV2 segment is serialized by the real writer; the method
// must derive its field set from the writer-produced FieldBinlogs and the public
// reader must physically read the rows back off the serialized bytes. Dispatch
// invariant: with an empty manifest the method must route to the binlog branch and
// succeed — a flip to the manifest branch would call GetManifestFieldIDs("") and
// error, which the NoError assertion catches.
func (s *BumpSchemaVersionCompactionTaskSuite) TestCompactionSegmentStorageFields_V2BinlogBranch_RealReadWrite() {
	cfg := s.task.compactionParams.StorageConfig
	schema := &schemapb.CollectionSchema{Fields: schemaBumpBaseFields()}
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Real StorageV2 write to the suite's real local storage. V2 has no manifest,
	// so writer.GetLogs() yields Manifest=="" + per-field binlogs on disk.
	const v2SegID = int64(200)
	segIDAlloc := allocator.NewLocalAllocator(v2SegID, math.MaxInt64)
	logIDAlloc := allocator.NewLocalAllocator(20000, 30000)
	v2Params := s.task.compactionParams
	v2Params.StorageVersion = storage.StorageV2
	writer, err := NewMultiSegmentWriter(context.Background(), s.mockBinlogIO,
		NewCompactionAllocator(segIDAlloc, logIDAlloc), 64*1024*1024, schema, v2Params,
		1000, PartitionID, CollectionID, "test_channel", compactionBatchSize,
		storage.WithStorageConfig(cfg), storage.WithVersion(storage.StorageV2))
	s.Require().NoError(err)
	for i := 0; i < 3; i++ {
		ts := int64(tsoutil.ComposeTSByTime(getMilvusBirthday()))
		s.Require().NoError(writer.WriteValue(&storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: ts,
			Value: map[int64]interface{}{
				common.RowIDField:     int64(i),
				common.TimeStampField: ts,
				100:                   int64(i),
				101:                   "v2 string " + string(rune('0'+i)),
			},
		}))
	}
	s.Require().NoError(writer.Close())
	written := writer.GetCompactionSegments()
	s.Require().Len(written, 1)
	s.Require().Empty(written[0].GetManifest(), "V2 segment must carry no manifest")

	seg := &datapb.CompactionSegmentBinlogs{
		CollectionID:   CollectionID,
		PartitionID:    PartitionID,
		SegmentID:      written[0].GetSegmentID(),
		FieldBinlogs:   written[0].GetInsertLogs(),
		InsertChannel:  "test_channel",
		Manifest:       "", // V2: no manifest -> binlog branch
		StorageVersion: storage.StorageV2,
	}

	// Method under test — no mock. The binlog branch is metadata-only -> NoError.
	got, err := compactionSegmentStorageFields(seg, cfg)
	s.Require().NoError(err)
	for _, fid := range []int64{common.RowIDField, common.TimeStampField, 100, 101} {
		s.Contains(got, fid, "V2 branch must surface writer-produced binlog field %d", fid)
	}
	s.NotContains(got, int64(777), "no phantom fields")

	// Public wrapper end-to-end: physically read every row back off disk.
	reader, existing, err := newCompactionSegmentRecordReader(context.Background(), seg, schema, cfg,
		storage.WithVersion(storage.StorageV2),
		storage.WithDownloader(s.task.chunkManager.MultiRead),
		storage.WithStorageConfig(cfg),
	)
	s.Require().NoError(err)
	defer reader.Close()
	s.Equal(got, existing, "wrapper must surface the same field set the method computed")
	rows := 0
	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		s.Require().NoError(err)
		rows += rec.Len()
	}
	s.Equal(3, rows, "3 rows physically read back from the real V2 segment")

	// Dispatch invariant (mutation-tight): empty manifest MUST route to the binlog
	// branch. A flip would call GetManifestFieldIDs("") and error; the NoError above
	// is the RED trip-wire. Assert the discriminating precondition explicitly.
	s.Empty(seg.GetManifest(), "V2 segment carries no manifest -> binlog branch is the only correct route")
}
