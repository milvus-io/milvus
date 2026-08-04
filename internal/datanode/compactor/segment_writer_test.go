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

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSegmentWriterSuite(t *testing.T) {
	suite.Run(t, new(SegmentWriteSuite))
}

type SegmentWriteSuite struct {
	suite.Suite
	collectionID int64
	parititonID  int64
}

func (s *SegmentWriteSuite) SetupSuite() {
	s.collectionID = 100
	s.parititonID = 101
}

func (s *SegmentWriteSuite) TestWriteFailed() {
	paramtable.Init()
	s.Run("get bm25 field failed", func() {
		schema := genCollectionSchemaWithBM25()
		// init segment writer with invalid bm25 fieldID
		writer, err := NewSegmentWriter(schema, 1024, compactionBatchSize, 1, s.parititonID, s.collectionID, []int64{1000})
		s.Require().NoError(err)

		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(0)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			Value:     genRowWithBM25(int64(0)),
		}
		err = writer.Write(&v)
		s.Error(err)
	})

	s.Run("parse bm25 field data failed", func() {
		schema := genCollectionSchemaWithBM25()
		// init segment writer with wrong field as bm25 sparse field
		writer, err := NewSegmentWriter(schema, 1024, compactionBatchSize, 1, s.parititonID, s.collectionID, []int64{101})
		s.Require().NoError(err)

		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(0)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			Value:     genRowWithBM25(int64(0)),
		}
		err = writer.Write(&v)
		s.Error(err)
	})
}

// TestRotateWriterPreservesRwOptions verifies that repeated rotateWriter() calls
// do not append WithUploader/WithVersion options to the stored w.rwOption slice.
// If they did, each subsequent rotateWriter() would accumulate extra options,
// causing duplicate uploader/version entries on writers created after the first rotation.
func (s *SegmentWriteSuite) TestRotateWriterPreservesRwOptions() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, s.T().TempDir())
	defer func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	}()

	mockBinlogIO := mock_util.NewMockBinlogIO(s.T())
	mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	mockAlloc := allocator.NewMockAllocator(s.T())
	var nextID int64 = 1000
	mockAlloc.EXPECT().AllocOne().RunAndReturn(func() (int64, error) {
		nextID++
		return nextID, nil
	}).Maybe()
	mockAlloc.EXPECT().Alloc(mock.Anything).RunAndReturn(func(n uint32) (int64, int64, error) {
		start := nextID
		nextID += int64(n)
		return start, nextID, nil
	}).Maybe()

	schema := genCollectionSchema()
	compAlloc := NewCompactionAllocator(mockAlloc, mockAlloc)
	params := compaction.GenParams()

	extraOpt := storage.WithStorageConfig(params.StorageConfig)
	writer, err := NewMultiSegmentWriter(
		context.Background(),
		mockBinlogIO,
		compAlloc,
		1024*1024,
		schema,
		params,
		1000,
		s.parititonID,
		s.collectionID,
		"test_channel",
		100,
		extraOpt,
	)
	s.Require().NoError(err)

	initialLen := len(writer.rwOption)

	// Rotate the writer multiple times; each rotation must NOT grow rwOption.
	for i := range 3 {
		err = writer.rotateWriter()
		s.Require().NoError(err)
		s.Equal(initialLen, len(writer.rwOption),
			"rwOption must not grow after rotateWriter (iteration %d)", i+1)
	}
}
