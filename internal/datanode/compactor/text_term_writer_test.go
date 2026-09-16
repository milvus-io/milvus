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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	flushio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestWriteCompactionTextTermsStorageV1(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	rootPath := t.TempDir()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, rootPath)
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	defer paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)

	schema := genCollectionSchemaWithBM25()
	schema.Functions[0].Params = append(schema.Functions[0].Params, &commonpb.KeyValuePair{
		Key:   common.EnableFuzzyKey,
		Value: "true",
	})

	const segmentID = int64(10)
	writer, err := NewSegmentWriter(schema, 100, compactionBatchSize, segmentID,
		PartitionID, CollectionID, []int64{102})
	require.NoError(t, err)
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday())
	require.NoError(t, writer.Write(&storage.Value{
		PK:        storage.NewInt64PrimaryKey(1),
		Timestamp: int64(ts),
		Value:     genRowWithBM25(1),
	}))
	writer.FlushAndIsFull()

	kvs, fieldBinlogs, err := serializeWrite(context.Background(),
		allocator.NewLocalAllocator(1000, math.MaxInt64), writer)
	require.NoError(t, err)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	require.NoError(t, cm.MultiWrite(context.Background(), kvs))

	params := compaction.GenParams()
	params.StorageConfig = &indexpb.StorageConfig{StorageType: "local", RootPath: rootPath}
	segment := &datapb.CompactionSegment{
		SegmentID:      segmentID,
		NumOfRows:      1,
		InsertLogs:     storage.SortFieldBinlogs(fieldBinlogs),
		StorageVersion: storage.StorageV1,
		Stats:          &datapb.Statistics{StatsBinlogSize: 42},
	}
	err = writeCompactionTextTerms(context.Background(), schema, segment,
		CollectionID, PartitionID, flushio.NewBinlogIO(cm),
		allocator.NewLocalAllocator(2000, math.MaxInt64), params)
	require.NoError(t, err)
	require.Len(t, segment.GetTextLogV2(), 1)
	require.EqualValues(t, 101, segment.GetTextLogV2()[0].GetFieldID())
	require.Equal(t, textindex.MilvusTextFstFormat, segment.GetTextLogV2()[0].GetFormat())
	require.Len(t, segment.GetTextLogV2()[0].GetBinlogs(), 1)
	fstLog := segment.GetTextLogV2()[0].GetBinlogs()[0]
	require.EqualValues(t, 1, fstLog.GetEntriesNum())
	require.Equal(t, ts, fstLog.GetTimestampTo())
	fstData, err := cm.Read(context.Background(), fstLog.GetLogPath())
	require.NoError(t, err)
	require.NotEmpty(t, fstData)
	require.Len(t, fstData, int(fstLog.GetLogSize()))
	require.Equal(t, int64(42+len(fstData)), segment.GetStats().GetStatsBinlogSize())
}
