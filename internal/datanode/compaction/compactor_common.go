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
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type EntityFilter struct {
	deletedPkTs map[interface{}]typeutil.Timestamp // pk2ts
	ttl         int64                              // nanoseconds
	currentTime time.Time

	expiredCount int
	deletedCount int
}

func newEntityFilter(deletedPkTs map[interface{}]typeutil.Timestamp, ttl int64, currTime time.Time) *EntityFilter {
	if deletedPkTs == nil {
		deletedPkTs = make(map[interface{}]typeutil.Timestamp)
	}
	return &EntityFilter{
		deletedPkTs: deletedPkTs,
		ttl:         ttl,
		currentTime: currTime,
	}
}

func (filter *EntityFilter) Filtered(pk any, ts typeutil.Timestamp) bool {
	if filter.isEntityDeleted(pk, ts) {
		filter.deletedCount++
		return true
	}

	// Filtering expired entity
	if filter.isEntityExpired(ts) {
		filter.expiredCount++
		return true
	}
	return false
}

func (filter *EntityFilter) GetExpiredCount() int {
	return filter.expiredCount
}

func (filter *EntityFilter) GetDeletedCount() int {
	return filter.deletedCount
}

func (filter *EntityFilter) GetDeltalogDeleteCount() int {
	return len(filter.deletedPkTs)
}

func (filter *EntityFilter) GetMissingDeleteCount() int {
	diff := filter.GetDeltalogDeleteCount() - filter.GetDeletedCount()
	if diff < 0 {
		diff = 0
	}
	return diff
}

func (filter *EntityFilter) isEntityDeleted(pk interface{}, pkTs typeutil.Timestamp) bool {
	if deleteTs, ok := filter.deletedPkTs[pk]; ok {
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if pkTs < deleteTs {
			return true
		}
	}
	return false
}

func (filter *EntityFilter) isEntityExpired(entityTs typeutil.Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if filter.ttl <= 0 {
		return false
	}
	entityTime, _ := tsoutil.ParseTS(entityTs)

	// this dur can represents 292 million years before or after 1970, enough for milvus
	// ttl calculation
	dur := filter.currentTime.UnixMilli() - entityTime.UnixMilli()

	// filter.ttl is nanoseconds
	return filter.ttl/int64(time.Millisecond) <= dur
}

func mergeDeltalogs(ctx context.Context, io io.BinlogIO, dpaths map[typeutil.UniqueID][]string) (map[interface{}]typeutil.Timestamp, error) {
	pk2ts := make(map[interface{}]typeutil.Timestamp)

	if len(dpaths) == 0 {
		log.Info("compact with no deltalogs, skip merge deltalogs")
		return pk2ts, nil
	}

	allIters := make([]*iter.DeltalogIterator, 0)
	for segID, paths := range dpaths {
		if len(paths) == 0 {
			continue
		}
		blobs, err := io.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download deltalogs",
				zap.Int64("segment", segID),
				zap.Strings("path", paths),
				zap.Error(err))
			return nil, err
		}

		allIters = append(allIters, iter.NewDeltalogIterator(blobs, nil))
	}

	for _, deltaIter := range allIters {
		for deltaIter.HasNext() {
			labeled, _ := deltaIter.Next()
			ts := labeled.GetTimestamp()
			if lastTs, ok := pk2ts[labeled.GetPk().GetValue()]; ok && lastTs > ts {
				ts = lastTs
			}
			pk2ts[labeled.GetPk().GetValue()] = ts
		}
	}

	log.Info("compact mergeDeltalogs end", zap.Int("delete entries counts", len(pk2ts)))
	return pk2ts, nil
}

func composePaths(segments []*datapb.CompactionSegmentBinlogs) (map[typeutil.UniqueID][]string, [][]string, error) {
	if err := binlog.DecompressCompactionBinlogs(segments); err != nil {
		log.Warn("compact wrong, fail to decompress compaction binlogs", zap.Error(err))
		return nil, nil, err
	}

	deltaPaths := make(map[typeutil.UniqueID][]string) // segmentID to deltalog paths
	allPath := make([][]string, 0)                     // group by binlog batch
	for _, s := range segments {
		// Get the batch count of field binlog files from non-empty segment
		// each segment might contain different batches
		var binlogBatchCount int
		for _, b := range s.GetFieldBinlogs() {
			if b != nil {
				binlogBatchCount = len(b.GetBinlogs())
				break
			}
		}
		if binlogBatchCount == 0 {
			log.Warn("compacting empty segment", zap.Int64("segmentID", s.GetSegmentID()))
			continue
		}

		for idx := 0; idx < binlogBatchCount; idx++ {
			var batchPaths []string
			for _, f := range s.GetFieldBinlogs() {
				batchPaths = append(batchPaths, f.GetBinlogs()[idx].GetLogPath())
			}
			allPath = append(allPath, batchPaths)
		}

		deltaPaths[s.GetSegmentID()] = []string{}
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				deltaPaths[s.GetSegmentID()] = append(deltaPaths[s.GetSegmentID()], l.GetLogPath())
			}
		}
	}
	return deltaPaths, allPath, nil
}

func serializeWrite(ctx context.Context, allocator allocator.Allocator, writer *SegmentWriter) (kvs map[string][]byte, fieldBinlogs map[int64]*datapb.FieldBinlog, err error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "serializeWrite")
	defer span.End()

	blobs, tr, err := writer.SerializeYield()
	startID, _, err := allocator.Alloc(uint32(len(blobs)))
	if err != nil {
		return nil, nil, err
	}

	kvs = make(map[string][]byte)
	fieldBinlogs = make(map[int64]*datapb.FieldBinlog)
	for i := range blobs {
		// Blob Key is generated by Serialize from int64 fieldID in collection schema, which won't raise error in ParseInt
		fID, _ := strconv.ParseInt(blobs[i].GetKey(), 10, 64)
		key, _ := binlog.BuildLogPath(storage.InsertBinlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), fID, startID+int64(i))

		kvs[key] = blobs[i].GetValue()
		fieldBinlogs[fID] = &datapb.FieldBinlog{
			FieldID: fID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:       int64(len(blobs[i].GetValue())),
					MemorySize:    blobs[i].GetMemorySize(),
					LogPath:       key,
					EntriesNum:    blobs[i].RowNum,
					TimestampFrom: tr.GetMinTimestamp(),
					TimestampTo:   tr.GetMaxTimestamp(),
				},
			},
		}
	}

	return
}

func statSerializeWrite(ctx context.Context, io io.BinlogIO, allocator allocator.Allocator, writer *SegmentWriter) (*datapb.FieldBinlog, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "statslog serializeWrite")
	defer span.End()
	sblob, err := writer.Finish()
	if err != nil {
		return nil, err
	}

	return uploadStatsBlobs(ctx, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), writer.GetPkID(), writer.GetRowNum(), io, allocator, sblob)
}

func uploadStatsBlobs(ctx context.Context, collectionID, partitionID, segmentID, pkID, numRows int64,
	io io.BinlogIO, allocator allocator.Allocator, blob *storage.Blob,
) (*datapb.FieldBinlog, error) {
	logID, err := allocator.AllocOne()
	if err != nil {
		return nil, err
	}

	key, _ := binlog.BuildLogPath(storage.StatsBinlog, collectionID, partitionID, segmentID, pkID, logID)
	kvs := map[string][]byte{key: blob.GetValue()}
	statFieldLog := &datapb.FieldBinlog{
		FieldID: pkID,
		Binlogs: []*datapb.Binlog{
			{
				LogSize:    int64(len(blob.GetValue())),
				MemorySize: int64(len(blob.GetValue())),
				LogPath:    key,
				EntriesNum: numRows,
			},
		},
	}
	if err := io.Upload(ctx, kvs); err != nil {
		log.Warn("failed to upload insert log", zap.Error(err))
		return nil, err
	}

	return statFieldLog, nil
}

func mergeFieldBinlogs(base, paths map[typeutil.UniqueID]*datapb.FieldBinlog) {
	for fID, fpath := range paths {
		if _, ok := base[fID]; !ok {
			base[fID] = &datapb.FieldBinlog{FieldID: fID, Binlogs: make([]*datapb.Binlog, 0)}
		}
		base[fID].Binlogs = append(base[fID].Binlogs, fpath.GetBinlogs()...)
	}
}
