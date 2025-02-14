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
	"strconv"
	"time"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/common"
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

// segmentRecordWriter is a wrapper of SegmentWriter to implement RecordWriter interface
type segmentRecordWriter struct {
	sw            *compaction.SegmentWriter
	binlogMaxSize uint64
	rootPath      string
	logID         int64
	maxLogID      int64
	binlogIO      io.BinlogIO
	ctx           context.Context
	numRows       int64
	bm25FieldIds  []int64

	lastUploads  []*conc.Future[any]
	binlogs      map[typeutil.UniqueID]*datapb.FieldBinlog
	statslog     *datapb.FieldBinlog
	bm25statslog []*datapb.FieldBinlog
}

var _ storage.RecordWriter = (*segmentRecordWriter)(nil)

func (srw *segmentRecordWriter) Close() error {
	if !srw.sw.FlushAndIsEmpty() {
		if err := srw.upload(); err != nil {
			return err
		}
		if err := srw.waitLastUpload(); err != nil {
			return err
		}
	}

	statslog, err := srw.statSerializeWrite()
	if err != nil {
		log.Ctx(srw.ctx).Warn("stats wrong, failed to serialize write segment stats",
			zap.Int64("remaining row count", srw.numRows), zap.Error(err))
		return err
	}
	srw.statslog = statslog
	srw.logID++

	if len(srw.bm25FieldIds) > 0 {
		binlogNums, bm25StatsLogs, err := srw.bm25SerializeWrite()
		if err != nil {
			log.Ctx(srw.ctx).Warn("compact wrong, failed to serialize write segment bm25 stats", zap.Error(err))
			return err
		}
		srw.logID += binlogNums
		srw.bm25statslog = bm25StatsLogs
	}

	return nil
}

func (srw *segmentRecordWriter) GetWrittenUncompressed() uint64 {
	return srw.sw.WrittenMemorySize()
}

func (srw *segmentRecordWriter) Write(r storage.Record) error {
	err := srw.sw.WriteRecord(r)
	if err != nil {
		return err
	}

	if srw.sw.IsFullWithBinlogMaxSize(srw.binlogMaxSize) {
		return srw.upload()
	}
	return nil
}

func (srw *segmentRecordWriter) upload() error {
	if err := srw.waitLastUpload(); err != nil {
		return err
	}
	binlogNum, kvs, partialBinlogs, err := serializeWrite(srw.ctx, srw.rootPath, srw.logID, srw.sw)
	if err != nil {
		return err
	}

	srw.lastUploads = srw.binlogIO.AsyncUpload(srw.ctx, kvs)
	if srw.binlogs == nil {
		srw.binlogs = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
	}
	mergeFieldBinlogs(srw.binlogs, partialBinlogs)

	srw.logID += binlogNum
	if srw.logID > srw.maxLogID {
		return fmt.Errorf("log id exausted")
	}
	return nil
}

func (srw *segmentRecordWriter) waitLastUpload() error {
	if len(srw.lastUploads) > 0 {
		for _, future := range srw.lastUploads {
			if _, err := future.Await(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (srw *segmentRecordWriter) statSerializeWrite() (*datapb.FieldBinlog, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(srw.ctx, "statslog serializeWrite")
	defer span.End()
	sblob, err := srw.sw.Finish()
	if err != nil {
		return nil, err
	}

	key, _ := binlog.BuildLogPathWithRootPath(srw.rootPath, storage.StatsBinlog,
		srw.sw.GetCollectionID(), srw.sw.GetPartitionID(), srw.sw.GetSegmentID(), srw.sw.GetPkID(), srw.logID)
	kvs := map[string][]byte{key: sblob.GetValue()}
	statFieldLog := &datapb.FieldBinlog{
		FieldID: srw.sw.GetPkID(),
		Binlogs: []*datapb.Binlog{
			{
				LogSize:    int64(len(sblob.GetValue())),
				MemorySize: int64(len(sblob.GetValue())),
				LogPath:    key,
				EntriesNum: srw.numRows,
			},
		},
	}
	if err := srw.binlogIO.Upload(ctx, kvs); err != nil {
		log.Ctx(ctx).Warn("failed to upload insert log", zap.Error(err))
		return nil, err
	}

	return statFieldLog, nil
}

func (srw *segmentRecordWriter) bm25SerializeWrite() (int64, []*datapb.FieldBinlog, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(srw.ctx, "bm25log serializeWrite")
	defer span.End()
	writer := srw.sw
	stats, err := writer.GetBm25StatsBlob()
	if err != nil {
		return 0, nil, err
	}

	kvs := make(map[string][]byte)
	binlogs := []*datapb.FieldBinlog{}
	cnt := int64(0)
	for fieldID, blob := range stats {
		key, _ := binlog.BuildLogPathWithRootPath(srw.rootPath, storage.BM25Binlog,
			writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), fieldID, srw.logID)
		kvs[key] = blob.GetValue()
		fieldLog := &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    int64(len(blob.GetValue())),
					MemorySize: int64(len(blob.GetValue())),
					LogPath:    key,
					EntriesNum: srw.numRows,
				},
			},
		}

		binlogs = append(binlogs, fieldLog)
		srw.logID++
		cnt++
	}

	if err := srw.binlogIO.Upload(ctx, kvs); err != nil {
		log.Ctx(ctx).Warn("failed to upload bm25 log", zap.Error(err))
		return 0, nil, err
	}

	return cnt, binlogs, nil
}

func (st *statsTask) sort(ctx context.Context) ([]*datapb.FieldBinlog, error) {
	numRows := st.req.GetNumRows()

	bm25FieldIds := compaction.GetBM25FieldIDs(st.req.GetSchema())
	pkField, err := typeutil.GetPrimaryFieldSchema(st.req.GetSchema())
	if err != nil {
		return nil, err
	}
	pkFieldID := pkField.FieldID
	writer, err := compaction.NewSegmentWriter(st.req.GetSchema(), numRows, statsBatchSize,
		st.req.GetTargetSegmentID(), st.req.GetPartitionID(), st.req.GetCollectionID(), bm25FieldIds)
	if err != nil {
		log.Ctx(ctx).Warn("sort segment wrong, unable to init segment writer",
			zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}
	srw := &segmentRecordWriter{
		sw:            writer,
		binlogMaxSize: st.req.GetBinlogMaxSize(),
		rootPath:      st.req.GetStorageConfig().GetRootPath(),
		logID:         st.req.StartLogID,
		maxLogID:      st.req.EndLogID,
		binlogIO:      st.binlogIO,
		ctx:           ctx,
		numRows:       st.req.NumRows,
		bm25FieldIds:  bm25FieldIds,
	}

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

	var isValueValid func(r storage.Record, ri, i int) bool
	switch pkField.DataType {
	case schemapb.DataType_Int64:
		isValueValid = func(r storage.Record, ri, i int) bool {
			v := r.Column(pkFieldID).(*array.Int64).Value(i)
			deleteTs, ok := deletePKs[v]
			ts := uint64(r.Column(common.TimeStampField).(*array.Int64).Value(i))
			if ok && ts < deleteTs {
				return false
			}
			return !st.isExpiredEntity(ts)
		}
	case schemapb.DataType_VarChar:
		isValueValid = func(r storage.Record, ri, i int) bool {
			v := r.Column(pkFieldID).(*array.String).Value(i)
			deleteTs, ok := deletePKs[v]
			ts := uint64(r.Column(common.TimeStampField).(*array.Int64).Value(i))
			if ok && ts < deleteTs {
				return false
			}
			return !st.isExpiredEntity(ts)
		}
	}

	downloadTimeCost := time.Duration(0)

	rrs := make([]storage.RecordReader, len(st.insertLogs))

	for i, paths := range st.insertLogs {
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

		rr, err := storage.NewCompositeBinlogRecordReader(st.req.Schema, storage.MakeBlobsReader(blobs))
		if err != nil {
			log.Warn("downloadData wrong, failed to new insert binlogs reader", zap.Error(err))
			return nil, err
		}
		rrs[i] = rr
	}

	log.Info("download data success",
		zap.Int64("numRows", numRows),
		zap.Duration("download binlogs elapse", downloadTimeCost),
	)

	numValidRows, err := storage.Sort(st.req.Schema, rrs, writer.GetPkID(), srw, isValueValid)
	if err != nil {
		log.Warn("sort failed", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}
	if err := srw.Close(); err != nil {
		return nil, err
	}

	insertLogs := lo.Values(srw.binlogs)
	if err := binlog.CompressFieldBinlogs(insertLogs); err != nil {
		return nil, err
	}

	statsLogs := []*datapb.FieldBinlog{srw.statslog}
	if err := binlog.CompressFieldBinlogs(statsLogs); err != nil {
		return nil, err
	}

	bm25StatsLogs := srw.bm25statslog
	if err := binlog.CompressFieldBinlogs(bm25StatsLogs); err != nil {
		return nil, err
	}

	st.node.storePKSortStatsResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		int64(numValidRows), insertLogs, statsLogs, bm25StatsLogs)

	log.Info("sort segment end",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.String("subTaskType", st.req.GetSubJobType().String()),
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Int64("old rows", numRows),
		zap.Int("valid rows", numValidRows))
	return insertLogs, nil
}

func (st *statsTask) Execute(ctx context.Context) error {
	// sort segment and check need to do text index.
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-Execute-%s-%d", st.req.GetClusterID(), st.req.GetTaskID()))
	defer span.End()

	insertLogs := st.req.GetInsertLogs()
	var err error
	if st.req.GetSubJobType() == indexpb.StatsSubJob_Sort {
		insertLogs, err = st.sort(ctx)
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
