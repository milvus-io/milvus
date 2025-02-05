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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datanode/compaction"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	_ "github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ task = (*statsTask)(nil)

const statsBatchSize = 100

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

	if err := binlog.DecompressBinLogWithRootPath(st.req.GetStorageConfig().GetRootPath(), storage.InsertBinlog, st.req.GetCollectionID(), st.req.GetPartitionID(),
		st.req.GetSegmentID(), st.req.GetInsertLogs()); err != nil {
		log.Ctx(ctx).Warn("Decompress insert binlog error", zap.Error(err))
		return err
	}

	if err := binlog.DecompressBinLogWithRootPath(st.req.GetStorageConfig().GetRootPath(), storage.DeleteBinlog, st.req.GetCollectionID(), st.req.GetPartitionID(),
		st.req.GetSegmentID(), st.req.GetDeltaLogs()); err != nil {
		log.Ctx(ctx).Warn("Decompress delta binlog error", zap.Error(err))
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

func (st *statsTask) sortSegment(ctx context.Context) ([]*datapb.FieldBinlog, error) {
	numRows := st.req.GetNumRows()

	bm25FieldIds := compaction.GetBM25FieldIDs(st.req.GetSchema())
	writer, err := compaction.NewSegmentWriter(st.req.GetSchema(), numRows, statsBatchSize, st.req.GetTargetSegmentID(), st.req.GetPartitionID(), st.req.GetCollectionID(), bm25FieldIds)
	if err != nil {
		log.Ctx(ctx).Warn("sort segment wrong, unable to init segment writer",
			zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}

	var (
		flushBatchCount int // binlog batch count

		allBinlogs    = make(map[typeutil.UniqueID]*datapb.FieldBinlog) // All binlog meta of a segment
		uploadFutures = make([]*conc.Future[any], 0)

		downloadCost     time.Duration
		serWriteTimeCost time.Duration
		sortTimeCost     time.Duration
	)

	downloadStart := time.Now()
	values, err := st.downloadData(ctx, numRows, writer.GetPkID(), bm25FieldIds)
	if err != nil {
		log.Ctx(ctx).Warn("download data failed", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}
	downloadCost = time.Since(downloadStart)

	sortStart := time.Now()
	sort.Slice(values, func(i, j int) bool {
		return values[i].PK.LT(values[j].PK)
	})
	sortTimeCost += time.Since(sortStart)

	for i, v := range values {
		err := writer.Write(v)
		if err != nil {
			log.Ctx(ctx).Warn("write value wrong, failed to writer row", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
			return nil, err
		}

		if (i+1)%statsBatchSize == 0 && writer.IsFullWithBinlogMaxSize(st.req.GetBinlogMaxSize()) {
			serWriteStart := time.Now()
			binlogNum, kvs, partialBinlogs, err := serializeWrite(ctx, st.req.GetStorageConfig().GetRootPath(), st.req.GetStartLogID()+st.logIDOffset, writer)
			if err != nil {
				log.Ctx(ctx).Warn("stats wrong, failed to serialize writer", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
				return nil, err
			}
			serWriteTimeCost += time.Since(serWriteStart)

			uploadFutures = append(uploadFutures, st.binlogIO.AsyncUpload(ctx, kvs)...)
			mergeFieldBinlogs(allBinlogs, partialBinlogs)

			flushBatchCount++
			st.logIDOffset += binlogNum
			if st.req.GetStartLogID()+st.logIDOffset >= st.req.GetEndLogID() {
				log.Ctx(ctx).Warn("binlog files too much, log is not enough", zap.Int64("taskID", st.req.GetTaskID()),
					zap.Int64("binlog num", binlogNum), zap.Int64("startLogID", st.req.GetStartLogID()),
					zap.Int64("endLogID", st.req.GetEndLogID()), zap.Int64("logIDOffset", st.logIDOffset))
				return nil, fmt.Errorf("binlog files too much, log is not enough")
			}
		}
	}

	if !writer.FlushAndIsEmpty() {
		serWriteStart := time.Now()
		binlogNum, kvs, partialBinlogs, err := serializeWrite(ctx, st.req.GetStorageConfig().GetRootPath(), st.req.GetStartLogID()+st.logIDOffset, writer)
		if err != nil {
			log.Ctx(ctx).Warn("stats wrong, failed to serialize writer", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
			return nil, err
		}
		serWriteTimeCost += time.Since(serWriteStart)
		st.logIDOffset += binlogNum

		uploadFutures = append(uploadFutures, st.binlogIO.AsyncUpload(ctx, kvs)...)
		mergeFieldBinlogs(allBinlogs, partialBinlogs)
		flushBatchCount++
	}

	err = conc.AwaitAll(uploadFutures...)
	if err != nil {
		log.Ctx(ctx).Warn("stats wrong, failed to upload kvs", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}

	serWriteStart := time.Now()
	binlogNums, sPath, err := statSerializeWrite(ctx, st.req.GetStorageConfig().GetRootPath(), st.binlogIO, st.req.GetStartLogID()+st.logIDOffset, writer, numRows)
	if err != nil {
		log.Ctx(ctx).Warn("stats wrong, failed to serialize write segment stats", zap.Int64("taskID", st.req.GetTaskID()),
			zap.Int64("remaining row count", numRows), zap.Error(err))
		return nil, err
	}
	serWriteTimeCost += time.Since(serWriteStart)

	st.logIDOffset += binlogNums

	var bm25StatsLogs []*datapb.FieldBinlog
	if len(bm25FieldIds) > 0 {
		binlogNums, bm25StatsLogs, err = bm25SerializeWrite(ctx, st.req.GetStorageConfig().GetRootPath(), st.binlogIO, st.req.GetStartLogID()+st.logIDOffset, writer, numRows)
		if err != nil {
			log.Ctx(ctx).Warn("compact wrong, failed to serialize write segment bm25 stats", zap.Error(err))
			return nil, err
		}
		st.logIDOffset += binlogNums

		if err := binlog.CompressFieldBinlogs(bm25StatsLogs); err != nil {
			return nil, err
		}
	}

	totalElapse := st.tr.RecordSpan()

	insertLogs := lo.Values(allBinlogs)
	if err := binlog.CompressFieldBinlogs(insertLogs); err != nil {
		return nil, err
	}

	statsLogs := []*datapb.FieldBinlog{sPath}
	if err := binlog.CompressFieldBinlogs(statsLogs); err != nil {
		return nil, err
	}

	st.node.storePKSortStatsResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		int64(len(values)), insertLogs, statsLogs, bm25StatsLogs)

	log.Ctx(ctx).Info("sort segment end",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.String("subTaskType", st.req.GetSubJobType().String()),
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Int64("old rows", numRows),
		zap.Int("valid rows", len(values)),
		zap.Int("binlog batch count", flushBatchCount),
		zap.Duration("download elapse", downloadCost),
		zap.Duration("sort elapse", sortTimeCost),
		zap.Duration("serWrite elapse", serWriteTimeCost),
		zap.Duration("total elapse", totalElapse))
	return insertLogs, nil
}

func (st *statsTask) Execute(ctx context.Context) error {
	// sort segment and check need to do text index.
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-Execute-%s-%d", st.req.GetClusterID(), st.req.GetTaskID()))
	defer span.End()

	insertLogs := st.req.GetInsertLogs()
	var err error
	if st.req.GetSubJobType() == indexpb.StatsSubJob_Sort {
		insertLogs, err = st.sortSegment(ctx)
		if err != nil {
			return err
		}
	}

	if st.req.GetSubJobType() == indexpb.StatsSubJob_Sort || st.req.GetSubJobType() == indexpb.StatsSubJob_TextIndexJob {
		err = st.createTextIndex(ctx,
			st.req.GetStorageConfig(),
			st.req.GetCollectionID(),
			st.req.GetPartitionID(),
			st.req.GetTargetSegmentID(),
			st.req.GetTaskVersion(),
			st.req.GetTaskID(),
			insertLogs)
		if err != nil {
			log.Ctx(ctx).Warn("stats wrong, failed to create text index", zap.Error(err))
			return err
		}
	} else if st.req.GetSubJobType() == indexpb.StatsSubJob_JsonKeyIndexJob {
		err = st.createJSONKeyIndex(ctx,
			st.req.GetStorageConfig(),
			st.req.GetCollectionID(),
			st.req.GetPartitionID(),
			st.req.GetTargetSegmentID(),
			st.req.GetTaskVersion(),
			st.req.GetTaskID(),
			insertLogs)
		if err != nil {
			log.Warn("stats wrong, failed to create json index", zap.Error(err))
			return err
		}
	}

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

func (st *statsTask) downloadData(ctx context.Context, numRows int64, PKFieldID int64, bm25FieldIds []int64) ([]*storage.Value, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Int64s("bm25Fields", bm25FieldIds),
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
					iter.Close()
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
		iter.Close()
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

func serializeWrite(ctx context.Context, rootPath string, startID int64, writer *compaction.SegmentWriter) (binlogNum int64, kvs map[string][]byte, fieldBinlogs map[int64]*datapb.FieldBinlog, err error) {
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
		key, _ := binlog.BuildLogPathWithRootPath(rootPath, storage.InsertBinlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), fID, startID+int64(i))

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

func statSerializeWrite(ctx context.Context, rootPath string, io io.BinlogIO, startID int64, writer *compaction.SegmentWriter, finalRowCount int64) (int64, *datapb.FieldBinlog, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "statslog serializeWrite")
	defer span.End()
	sblob, err := writer.Finish()
	if err != nil {
		return 0, nil, err
	}

	binlogNum := int64(1)
	key, _ := binlog.BuildLogPathWithRootPath(rootPath, storage.StatsBinlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), writer.GetPkID(), startID)
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
		log.Ctx(ctx).Warn("failed to upload insert log", zap.Error(err))
		return binlogNum, nil, err
	}

	return binlogNum, statFieldLog, nil
}

func bm25SerializeWrite(ctx context.Context, rootPath string, io io.BinlogIO, startID int64, writer *compaction.SegmentWriter, finalRowCount int64) (int64, []*datapb.FieldBinlog, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "bm25log serializeWrite")
	defer span.End()
	stats, err := writer.GetBm25StatsBlob()
	if err != nil {
		return 0, nil, err
	}

	kvs := make(map[string][]byte)
	binlogs := []*datapb.FieldBinlog{}
	cnt := int64(0)
	for fieldID, blob := range stats {
		key, _ := binlog.BuildLogPathWithRootPath(rootPath, storage.BM25Binlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), fieldID, startID+cnt)
		kvs[key] = blob.GetValue()
		fieldLog := &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    int64(len(blob.GetValue())),
					MemorySize: int64(len(blob.GetValue())),
					LogPath:    key,
					EntriesNum: finalRowCount,
				},
			},
		}

		binlogs = append(binlogs, fieldLog)
		cnt++
	}

	if err := io.Upload(ctx, kvs); err != nil {
		log.Ctx(ctx).Warn("failed to upload bm25 log", zap.Error(err))
		return 0, nil, err
	}

	return cnt, binlogs, nil
}

func ParseStorageConfig(s *indexpb.StorageConfig) (*indexcgopb.StorageConfig, error) {
	bs, err := proto.Marshal(s)
	if err != nil {
		return nil, err
	}
	res := &indexcgopb.StorageConfig{}
	err = proto.Unmarshal(bs, res)
	return res, err
}

func (st *statsTask) createTextIndex(ctx context.Context,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	version int64,
	taskID int64,
	insertBinlogs []*datapb.FieldBinlog,
) error {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	fieldBinlogs := lo.GroupBy(insertBinlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64) ([]string, error) {
		binlogs, ok := fieldBinlogs[fieldID]
		if !ok {
			return nil, fmt.Errorf("field binlog not found for field %d", fieldID)
		}
		result := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, file := range binlog.GetBinlogs() {
				result = append(result, metautil.BuildInsertLogPath(storageConfig.GetRootPath(), collectionID, partitionID, segmentID, fieldID, file.GetLogID()))
			}
		}
		return result, nil
	}

	newStorageConfig, err := ParseStorageConfig(storageConfig)
	if err != nil {
		return err
	}

	textIndexLogs := make(map[int64]*datapb.TextIndexStats)
	for _, field := range st.req.GetSchema().GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableMatch() {
			continue
		}
		log.Info("field enable match, ready to create text index", zap.Int64("field id", field.GetFieldID()))
		// create text index and upload the text index files.
		files, err := getInsertFiles(field.GetFieldID())
		if err != nil {
			return err
		}

		buildIndexParams := &indexcgopb.BuildIndexInfo{
			BuildID:       taskID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			SegmentID:     segmentID,
			IndexVersion:  version,
			InsertFiles:   files,
			FieldSchema:   field,
			StorageConfig: newStorageConfig,
		}

		uploaded, err := indexcgowrapper.CreateTextIndex(ctx, buildIndexParams)
		if err != nil {
			return err
		}
		textIndexLogs[field.GetFieldID()] = &datapb.TextIndexStats{
			FieldID: field.GetFieldID(),
			Version: version,
			BuildID: taskID,
			Files:   lo.Keys(uploaded),
		}
		elapse := st.tr.RecordSpan()
		log.Info("field enable match, create text index done",
			zap.Int64("targetSegmentID", st.req.GetTargetSegmentID()),
			zap.Int64("field id", field.GetFieldID()),
			zap.Strings("files", lo.Keys(uploaded)),
			zap.Duration("elapse", elapse),
		)
	}

	st.node.storeStatsTextIndexResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		textIndexLogs)
	return nil
}

func (st *statsTask) createJSONKeyIndex(ctx context.Context,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	version int64,
	taskID int64,
	insertBinlogs []*datapb.FieldBinlog,
) error {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Any("statsJobType", st.req.GetSubJobType()),
	)

	fieldBinlogs := lo.GroupBy(insertBinlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64) ([]string, error) {
		binlogs, ok := fieldBinlogs[fieldID]
		if !ok {
			return nil, fmt.Errorf("field binlog not found for field %d", fieldID)
		}
		result := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, file := range binlog.GetBinlogs() {
				result = append(result, metautil.BuildInsertLogPath(storageConfig.GetRootPath(), collectionID, partitionID, segmentID, fieldID, file.GetLogID()))
			}
		}
		return result, nil
	}

	newStorageConfig, err := ParseStorageConfig(storageConfig)
	if err != nil {
		return err
	}

	jsonKeyIndexStats := make(map[int64]*datapb.JsonKeyStats)
	for _, field := range st.req.GetSchema().GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableJSONKeyIndex() {
			continue
		}
		log.Info("field enable json key index, ready to create json key index", zap.Int64("field id", field.GetFieldID()))
		files, err := getInsertFiles(field.GetFieldID())
		if err != nil {
			return err
		}

		buildIndexParams := &indexcgopb.BuildIndexInfo{
			BuildID:       taskID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			SegmentID:     segmentID,
			IndexVersion:  version,
			InsertFiles:   files,
			FieldSchema:   field,
			StorageConfig: newStorageConfig,
		}

		uploaded, err := indexcgowrapper.CreateJSONKeyIndex(ctx, buildIndexParams)
		if err != nil {
			return err
		}
		jsonKeyIndexStats[field.GetFieldID()] = &datapb.JsonKeyStats{
			FieldID: field.GetFieldID(),
			Version: version,
			BuildID: taskID,
			Files:   lo.Keys(uploaded),
		}
		log.Info("field enable json key index, create json key index done",
			zap.Int64("field id", field.GetFieldID()),
			zap.Strings("files", lo.Keys(uploaded)),
		)
	}

	totalElapse := st.tr.RecordSpan()

	st.node.storeJSONKeyIndexResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		jsonKeyIndexStats)

	log.Info("create json key index done",
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Duration("total elapse", totalElapse))
	return nil
}
