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

package indexnode

import (
	"context"
	"fmt"
	sio "io"
	"sort"
	"strconv"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	_ "github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ task = (*statsTask)(nil)

type statsTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *workerpb.CreateStatsRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	node     *IndexNode
	binlogIO io.BinlogIO

	insertLogs  [][]string
	deltaLogs   []string
	logIDOffset int64
}

func newStatsTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.CreateStatsRequest,
	node *IndexNode,
	binlogIO io.BinlogIO,
) *statsTask {
	return &statsTask{
		ident:       fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetTaskID()),
		ctx:         ctx,
		cancel:      cancel,
		req:         req,
		node:        node,
		binlogIO:    binlogIO,
		tr:          timerecord.NewTimeRecorder(fmt.Sprintf("ClusterID: %s, TaskID: %d", req.GetClusterID(), req.GetTaskID())),
		logIDOffset: 0,
	}
}

func (st *statsTask) Ctx() context.Context {
	return st.ctx
}

func (st *statsTask) Name() string {
	return st.ident
}

func (st *statsTask) OnEnqueue(ctx context.Context) error {
	st.queueDur = 0
	st.tr.RecordSpan()
	log.Ctx(ctx).Info("statsTask enqueue", zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()))
	return nil
}

func (st *statsTask) SetState(state indexpb.JobState, failReason string) {
	st.node.storeStatsTaskState(st.req.GetClusterID(), st.req.GetTaskID(), state, failReason)
}

func (st *statsTask) GetState() indexpb.JobState {
	return st.node.getStatsTaskState(st.req.GetClusterID(), st.req.GetTaskID())
}

func (st *statsTask) PreExecute(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-PreExecute-%s-%d", st.req.GetClusterID(), st.req.GetTaskID()))
	defer span.End()

	st.queueDur = st.tr.RecordSpan()
	log.Ctx(ctx).Info("Begin to prepare stats task",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	if err := binlog.DecompressBinLog(storage.InsertBinlog, st.req.GetCollectionID(), st.req.GetPartitionID(),
		st.req.GetSegmentID(), st.req.GetInsertLogs()); err != nil {
		log.Warn("Decompress insert binlog error", zap.Error(err))
		return err
	}

	if err := binlog.DecompressBinLog(storage.DeleteBinlog, st.req.GetCollectionID(), st.req.GetPartitionID(),
		st.req.GetSegmentID(), st.req.GetDeltaLogs()); err != nil {
		log.Warn("Decompress delta binlog error", zap.Error(err))
		return err
	}

	st.insertLogs = make([][]string, 0)
	binlogNum := len(st.req.GetInsertLogs()[0].GetBinlogs())
	for idx := 0; idx < binlogNum; idx++ {
		var batchPaths []string
		for _, f := range st.req.GetInsertLogs() {
			batchPaths = append(batchPaths, f.GetBinlogs()[idx].GetLogPath())
		}
		st.insertLogs = append(st.insertLogs, batchPaths)
	}

	for _, d := range st.req.GetDeltaLogs() {
		for _, l := range d.GetBinlogs() {
			st.deltaLogs = append(st.deltaLogs, l.GetLogPath())
		}
	}

	return nil
}

func (st *statsTask) Execute(ctx context.Context) error {
	// sort segment and check need to do text index.
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-Execute-%s-%d", st.req.GetClusterID(), st.req.GetTaskID()))
	defer span.End()
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	numRows := st.req.GetNumRows()
	writer, err := compaction.NewSegmentWriter(st.req.GetSchema(), numRows, st.req.GetTargetSegmentID(), st.req.GetPartitionID(), st.req.GetCollectionID())
	if err != nil {
		log.Warn("sort segment wrong, unable to init segment writer", zap.Error(err))
		return err
	}

	var (
		flushBatchCount   int   // binlog batch count
		unFlushedRowCount int64 = 0

		// All binlog meta of a segment
		allBinlogs = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
	)

	serWriteTimeCost := time.Duration(0)
	uploadTimeCost := time.Duration(0)
	sortTimeCost := time.Duration(0)

	values, err := st.downloadData(ctx, numRows, writer.GetPkID())
	if err != nil {
		log.Warn("download data failed", zap.Error(err))
		return err
	}

	sortStart := time.Now()
	sort.Slice(values, func(i, j int) bool {
		return values[i].PK.LT(values[j].PK)
	})
	sortTimeCost += time.Since(sortStart)

	for _, v := range values {
		err := writer.Write(v)
		if err != nil {
			log.Warn("write value wrong, failed to writer row", zap.Error(err))
			return err
		}
		unFlushedRowCount++

		if (unFlushedRowCount+1)%100 == 0 && writer.FlushAndIsFullWithBinlogMaxSize(st.req.GetBinlogMaxSize()) {
			serWriteStart := time.Now()
			binlogNum, kvs, partialBinlogs, err := serializeWrite(ctx, st.req.GetStartLogID()+st.logIDOffset, writer)
			if err != nil {
				log.Warn("stats wrong, failed to serialize writer", zap.Error(err))
				return err
			}
			serWriteTimeCost += time.Since(serWriteStart)

			uploadStart := time.Now()
			if err := st.binlogIO.Upload(ctx, kvs); err != nil {
				log.Warn("stats wrong, failed to upload kvs", zap.Error(err))
				return err
			}
			uploadTimeCost += time.Since(uploadStart)

			mergeFieldBinlogs(allBinlogs, partialBinlogs)

			flushBatchCount++
			unFlushedRowCount = 0
			st.logIDOffset += binlogNum
			if st.req.GetStartLogID()+st.logIDOffset >= st.req.GetEndLogID() {
				log.Warn("binlog files too much, log is not enough",
					zap.Int64("binlog num", binlogNum), zap.Int64("startLogID", st.req.GetStartLogID()),
					zap.Int64("endLogID", st.req.GetEndLogID()), zap.Int64("logIDOffset", st.logIDOffset))
				return fmt.Errorf("binlog files too much, log is not enough")
			}
		}
	}

	if !writer.FlushAndIsEmpty() {
		serWriteStart := time.Now()
		binlogNum, kvs, partialBinlogs, err := serializeWrite(ctx, st.req.GetStartLogID()+st.logIDOffset, writer)
		if err != nil {
			log.Warn("stats wrong, failed to serialize writer", zap.Error(err))
			return err
		}
		serWriteTimeCost += time.Since(serWriteStart)
		st.logIDOffset += binlogNum

		uploadStart := time.Now()
		if err := st.binlogIO.Upload(ctx, kvs); err != nil {
			return err
		}
		uploadTimeCost += time.Since(uploadStart)

		mergeFieldBinlogs(allBinlogs, partialBinlogs)
		flushBatchCount++
	}

	serWriteStart := time.Now()
	binlogNums, sPath, err := statSerializeWrite(ctx, st.binlogIO, st.req.GetStartLogID()+st.logIDOffset, writer, numRows)
	if err != nil {
		log.Warn("stats wrong, failed to serialize write segment stats",
			zap.Int64("remaining row count", numRows), zap.Error(err))
		return err
	}
	serWriteTimeCost += time.Since(serWriteStart)

	st.logIDOffset += binlogNums

	totalElapse := st.tr.RecordSpan()

	insertLogs := lo.Values(allBinlogs)
	if err := binlog.CompressFieldBinlogs(insertLogs); err != nil {
		return err
	}

	statsLogs := []*datapb.FieldBinlog{sPath}
	if err := binlog.CompressFieldBinlogs(statsLogs); err != nil {
		return err
	}

	log.Info("sort segment end",
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Int64("old rows", numRows),
		zap.Int("valid rows", len(values)),
		zap.Int("binlog batch count", flushBatchCount),
		zap.Duration("upload binlogs elapse", uploadTimeCost),
		zap.Duration("sort elapse", sortTimeCost),
		zap.Duration("serWrite elapse", serWriteTimeCost),
		zap.Duration("total elapse", totalElapse))

	textIndexStatsLogs, err := st.createTextIndex(ctx,
		st.req.GetStorageConfig(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetTaskVersion(),
		lo.Values(allBinlogs))
	if err != nil {
		log.Warn("stats wrong, failed to create text index", zap.Error(err))
		return err
	}

	st.node.storeStatsResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		int64(len(values)), insertLogs, statsLogs, textIndexStatsLogs)

	return nil
}

func (st *statsTask) PostExecute(ctx context.Context) error {
	return nil
}

func (st *statsTask) Reset() {
	st.ident = ""
	st.ctx = nil
	st.req = nil
	st.cancel = nil
	st.tr = nil
	st.node = nil
}

func (st *statsTask) downloadData(ctx context.Context, numRows int64, PKFieldID int64) ([]*storage.Value, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	deletePKs, err := st.loadDeltalogs(ctx, st.deltaLogs)
	if err != nil {
		log.Warn("load deletePKs failed", zap.Error(err))
		return nil, err
	}

	var (
		remainingRowCount int64 // the number of remaining entities
		expiredRowCount   int64 // the number of expired entities
	)

	isValueDeleted := func(v *storage.Value) bool {
		ts, ok := deletePKs[v.PK.GetValue()]
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if ok && uint64(v.Timestamp) < ts {
			return true
		}
		return false
	}

	downloadTimeCost := time.Duration(0)

	values := make([]*storage.Value, 0, numRows)
	for _, paths := range st.insertLogs {
		log := log.With(zap.Strings("paths", paths))
		downloadStart := time.Now()
		allValues, err := st.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("download wrong, fail to download insertLogs", zap.Error(err))
			return nil, err
		}
		downloadTimeCost += time.Since(downloadStart)

		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})

		iter, err := storage.NewBinlogDeserializeReader(blobs, PKFieldID)
		if err != nil {
			log.Warn("downloadData wrong, failed to new insert binlogs reader", zap.Error(err))
			return nil, err
		}

		for {
			err := iter.Next()
			if err != nil {
				if err == sio.EOF {
					break
				} else {
					log.Warn("downloadData wrong, failed to iter through data", zap.Error(err))
					return nil, err
				}
			}

			v := iter.Value()
			if isValueDeleted(v) {
				continue
			}

			// Filtering expired entity
			if st.isExpiredEntity(typeutil.Timestamp(v.Timestamp)) {
				expiredRowCount++
				continue
			}

			values = append(values, iter.Value())
			remainingRowCount++
		}
	}

	log.Info("download data success",
		zap.Int64("old rows", numRows),
		zap.Int64("remainingRowCount", remainingRowCount),
		zap.Int64("expiredRowCount", expiredRowCount),
		zap.Duration("download binlogs elapse", downloadTimeCost),
	)
	return values, nil
}

func (st *statsTask) loadDeltalogs(ctx context.Context, dpaths []string) (map[interface{}]typeutil.Timestamp, error) {
	st.tr.RecordSpan()
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "loadDeltalogs")
	defer span.End()

	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	pk2ts := make(map[interface{}]typeutil.Timestamp)

	if len(dpaths) == 0 {
		log.Info("compact with no deltalogs, skip merge deltalogs")
		return pk2ts, nil
	}

	blobs, err := st.binlogIO.Download(ctx, dpaths)
	if err != nil {
		log.Warn("compact wrong, fail to download deltalogs", zap.Error(err))
		return nil, err
	}

	deltaIter := iter.NewDeltalogIterator(blobs, nil)
	for deltaIter.HasNext() {
		labeled, _ := deltaIter.Next()
		ts := labeled.GetTimestamp()
		if lastTs, ok := pk2ts[labeled.GetPk().GetValue()]; ok && lastTs > ts {
			ts = lastTs
		}
		pk2ts[labeled.GetPk().GetValue()] = ts
	}

	log.Info("compact loadDeltalogs end",
		zap.Int("deleted pk counts", len(pk2ts)),
		zap.Duration("elapse", st.tr.RecordSpan()))

	return pk2ts, nil
}

func (st *statsTask) isExpiredEntity(ts typeutil.Timestamp) bool {
	now := st.req.GetCurrentTs()

	// entity expire is not enabled if duration <= 0
	if st.req.GetCollectionTtl() <= 0 {
		return false
	}

	entityT, _ := tsoutil.ParseTS(ts)
	nowT, _ := tsoutil.ParseTS(now)

	return entityT.Add(time.Duration(st.req.GetCollectionTtl())).Before(nowT)
}

func mergeFieldBinlogs(base, paths map[typeutil.UniqueID]*datapb.FieldBinlog) {
	for fID, fpath := range paths {
		if _, ok := base[fID]; !ok {
			base[fID] = &datapb.FieldBinlog{FieldID: fID, Binlogs: make([]*datapb.Binlog, 0)}
		}
		base[fID].Binlogs = append(base[fID].Binlogs, fpath.GetBinlogs()...)
	}
}

func serializeWrite(ctx context.Context, startID int64, writer *compaction.SegmentWriter) (binlogNum int64, kvs map[string][]byte, fieldBinlogs map[int64]*datapb.FieldBinlog, err error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "serializeWrite")
	defer span.End()

	blobs, tr, err := writer.SerializeYield()
	if err != nil {
		return 0, nil, nil, err
	}

	binlogNum = int64(len(blobs))
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

func statSerializeWrite(ctx context.Context, io io.BinlogIO, startID int64, writer *compaction.SegmentWriter, finalRowCount int64) (int64, *datapb.FieldBinlog, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "statslog serializeWrite")
	defer span.End()
	sblob, err := writer.Finish()
	if err != nil {
		return 0, nil, err
	}

	binlogNum := int64(1)
	key, _ := binlog.BuildLogPath(storage.StatsBinlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), writer.GetPkID(), startID)
	kvs := map[string][]byte{key: sblob.GetValue()}
	statFieldLog := &datapb.FieldBinlog{
		FieldID: writer.GetPkID(),
		Binlogs: []*datapb.Binlog{
			{
				LogSize:    int64(len(sblob.GetValue())),
				MemorySize: int64(len(sblob.GetValue())),
				LogPath:    key,
				EntriesNum: finalRowCount,
			},
		},
	}
	if err := io.Upload(ctx, kvs); err != nil {
		log.Warn("failed to upload insert log", zap.Error(err))
		return binlogNum, nil, err
	}

	return binlogNum, statFieldLog, nil
}

func buildTextLogPrefix(rootPath string, collID, partID, segID, fieldID, version int64) string {
	return fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d", rootPath, common.TextIndexPath, collID, partID, segID, fieldID, version)
}

func (st *statsTask) createTextIndex(ctx context.Context,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	version int64,
	insertBinlogs []*datapb.FieldBinlog,
) (map[int64]*datapb.TextIndexStats, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	fieldStatsLogs := make(map[int64]*datapb.TextIndexStats)
	for _, field := range st.req.GetSchema().GetFields() {
		if field.GetDataType() == schemapb.DataType_VarChar {
			for _, binlog := range insertBinlogs {
				if binlog.GetFieldID() == field.GetFieldID() {
					// do text index
					_ = buildTextLogPrefix(storageConfig.GetRootPath(), collectionID, partitionID, segmentID, field.GetFieldID(), version)
					fieldStatsLogs[field.GetFieldID()] = &datapb.TextIndexStats{
						Version: version,
						Files:   nil,
					}
					log.Info("TODO: call CGO CreateTextIndex", zap.Int64("fieldID", field.GetFieldID()))
					break
				}
			}
		}
	}

	totalElapse := st.tr.RecordSpan()

	log.Info("create text index done",
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Duration("total elapse", totalElapse))
	return fieldStatsLogs, nil
}
