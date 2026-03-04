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

package index

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	_ "github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Task = (*statsTask)(nil)

const statsBatchSize = 100

type statsTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *workerpb.CreateStatsRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	manager  *TaskManager
	binlogIO io.BinlogIO
	cm       storage.ChunkManager

	logIDOffset int64
	currentTime time.Time
}

type BuildIndexOptions struct {
	TantivyMemory                int64
	JsonStatsMaxShreddingColumns int64
	JsonStatsShreddingRatio      float64
	JsonStatsWriteBatchSize      int64
}

func NewStatsTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.CreateStatsRequest,
	manager *TaskManager,
	cm storage.ChunkManager,
) *statsTask {
	return &statsTask{
		ident:       fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetTaskID()),
		ctx:         ctx,
		cancel:      cancel,
		req:         req,
		manager:     manager,
		binlogIO:    io.NewBinlogIO(cm),
		cm:          cm,
		tr:          timerecord.NewTimeRecorder(fmt.Sprintf("ClusterID: %s, TaskID: %d", req.GetClusterID(), req.GetTaskID())),
		currentTime: tsoutil.PhysicalTime(req.GetCurrentTs()),
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
	log.Ctx(ctx).Info("statsTask enqueue",
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()))
	return nil
}

func (st *statsTask) SetState(state indexpb.JobState, failReason string) {
	st.manager.StoreStatsTaskState(st.req.GetClusterID(), st.req.GetTaskID(), state, failReason)
}

func (st *statsTask) GetState() indexpb.JobState {
	return st.manager.GetStatsTaskState(st.req.GetClusterID(), st.req.GetTaskID())
}

func (st *statsTask) GetSlot() int64 {
	return st.req.GetTaskSlot()
}

func (st *statsTask) IsVectorIndex() bool {
	return false
}

func (st *statsTask) PreExecute(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-PreExecute-%s-%d", st.req.GetClusterID(), st.req.GetTaskID()))
	defer span.End()

	st.queueDur = st.tr.RecordSpan()
	log.Ctx(ctx).Info("Begin to PreExecute stats task",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Int64("queue duration(ms)", st.queueDur.Milliseconds()),
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

	preExecuteRecordSpan := st.tr.RecordSpan()

	log.Ctx(ctx).Info("successfully PreExecute stats task",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Int64("storageVersion", st.req.GetStorageVersion()),
		zap.Int64("preExecuteRecordSpan(ms)", preExecuteRecordSpan.Milliseconds()),
		zap.Any("storageConfig", st.req.StorageConfig),
	)
	return nil
}

func (st *statsTask) sort(ctx context.Context) ([]*datapb.FieldBinlog, error) {
	numRows := st.req.GetNumRows()
	pkField, err := typeutil.GetPrimaryFieldSchema(st.req.GetSchema())
	if err != nil {
		return nil, err
	}

	alloc := allocator.NewLocalAllocator(st.req.StartLogID, st.req.EndLogID)
	srw, err := storage.NewBinlogRecordWriter(ctx,
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetSchema(),
		alloc,
		st.req.GetBinlogMaxSize(),
		numRows,
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return st.binlogIO.Upload(ctx, kvs)
		}),
		storage.WithVersion(st.req.GetStorageVersion()),
		storage.WithStorageConfig(st.req.GetStorageConfig()),
	)
	if err != nil {
		log.Ctx(ctx).Warn("sort segment wrong, unable to init segment writer",
			zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}

	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
	)

	deletePKs, err := compaction.ComposeDeleteFromDeltalogsV1(ctx, pkField.DataType, st.req.GetDeltaLogs(),
		storage.WithDownloader(st.binlogIO.Download),
		storage.WithStorageConfig(st.req.GetStorageConfig()))
	if err != nil {
		log.Warn("load deletePKs failed", zap.Error(err))
		return nil, err
	}

	entityFilter := compaction.NewEntityFilter(deletePKs, st.req.GetCollectionTtl(), st.currentTime)

	var predicate func(r storage.Record, ri, i int) bool
	switch pkField.DataType {
	case schemapb.DataType_Int64:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.Int64).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			return !entityFilter.Filtered(pk, uint64(ts), -1)
		}
	case schemapb.DataType_VarChar:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.String).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			return !entityFilter.Filtered(pk, uint64(ts), -1)
		}
	default:
		log.Warn("sort task only support int64 and varchar pk field")
	}

	rr, err := storage.NewBinlogRecordReader(ctx, st.req.InsertLogs, st.req.Schema,
		storage.WithCollectionID(st.req.CollectionID),
		storage.WithVersion(st.req.StorageVersion),
		storage.WithDownloader(st.binlogIO.Download),
		storage.WithStorageConfig(st.req.GetStorageConfig()),
	)
	if err != nil {
		log.Warn("error creating insert binlog reader", zap.Error(err))
		return nil, err
	}
	defer rr.Close()

	rrs := []storage.RecordReader{rr}
	numValidRows, _, err := storage.Sort(st.req.GetBinlogMaxSize(), st.req.GetSchema(), rrs, srw, predicate, []int64{pkField.FieldID})
	if err != nil {
		log.Warn("sort failed", zap.Int64("taskID", st.req.GetTaskID()), zap.Error(err))
		return nil, err
	}
	if err := srw.Close(); err != nil {
		return nil, err
	}

	binlogs, stats, bm25stats, _, _ := srw.GetLogs()
	insertLogs := storage.SortFieldBinlogs(binlogs)
	if err := binlog.CompressFieldBinlogs(insertLogs); err != nil {
		return nil, err
	}

	statsLogs := []*datapb.FieldBinlog{stats}
	if err := binlog.CompressFieldBinlogs(statsLogs); err != nil {
		return nil, err
	}

	bm25StatsLogs := lo.Values(bm25stats)
	if err := binlog.CompressFieldBinlogs(bm25StatsLogs); err != nil {
		return nil, err
	}

	st.manager.StorePKSortStatsResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		int64(numValidRows), insertLogs, statsLogs, bm25StatsLogs)

	debug.FreeOSMemory()
	elapse := st.tr.RecordSpan()
	log.Info("sort segment end",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.String("subTaskType", st.req.GetSubJobType().String()),
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Int64("old rows", numRows),
		zap.Int("valid rows", numValidRows),
		zap.Duration("elapse", elapse),
	)
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

	if len(insertLogs) == 0 {
		log.Ctx(ctx).Info("there is no insertBinlogs, skip creating text index")
		return nil
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
	if (st.req.EnableJsonKeyStatsInSort && st.req.GetSubJobType() == indexpb.StatsSubJob_Sort) || st.req.GetSubJobType() == indexpb.StatsSubJob_JsonKeyIndexJob {
		if !st.req.GetEnableJsonKeyStats() {
			return nil
		}

		// for compatibility, we only support json data format version 2 and above after 2.6
		// for old version, we skip creating json key index
		if st.req.GetJsonKeyStatsDataFormat() < 2 {
			log.Ctx(ctx).Info("json data format version is too old, skip creating json key index", zap.Int64("data format", st.req.GetJsonKeyStatsDataFormat()))
			return nil
		}

		err = st.createJSONKeyStats(ctx,
			st.req.GetStorageConfig(),
			st.req.GetCollectionID(),
			st.req.GetPartitionID(),
			st.req.GetTargetSegmentID(),
			st.req.GetTaskVersion(),
			st.req.GetTaskID(),
			st.req.GetJsonKeyStatsDataFormat(),
			insertLogs,
			st.req.GetJsonStatsMaxShreddingColumns(),
			st.req.GetJsonStatsShreddingRatioThreshold(),
			st.req.GetJsonStatsWriteBatchSize())
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
	st.manager = nil
}

func serializeWrite(ctx context.Context, rootPath string, startID int64, writer *compactor.SegmentWriter) (binlogNum int64, kvs map[string][]byte, fieldBinlogs map[int64]*datapb.FieldBinlog, err error) {
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
		zap.Int64("storageVersion", st.req.GetStorageVersion()),
	)

	fieldBinlogs := lo.GroupBy(insertBinlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64, enableNull bool) ([]string, error) {
		if st.req.GetStorageVersion() == storage.StorageV2 || st.req.GetStorageVersion() == storage.StorageV3 {
			return []string{}, nil
		}
		binlogs, ok := fieldBinlogs[fieldID]
		if !ok && !enableNull {
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

	// Concurrent create text index for all match-enabled fields
	var (
		mu            sync.Mutex
		textIndexLogs = make(map[int64]*datapb.TextIndexStats)
	)

	eg, egCtx := errgroup.WithContext(ctx)

	var analyzerExtraInfo string
	if len(st.req.GetFileResources()) > 0 {
		err := fileresource.GlobalFileManager.Download(ctx, st.cm, st.req.GetFileResources()...)
		if err != nil {
			return err
		}
		defer fileresource.GlobalFileManager.Release(st.req.GetFileResources()...)
		analyzerExtraInfo, err = analyzer.BuildExtraResourceInfo(st.req.GetStorageConfig().GetRootPath(), st.req.GetFileResources())
		if err != nil {
			return err
		}
	}

	for _, field := range st.req.GetSchema().GetFields() {
		field := field
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableMatch() {
			continue
		}
		log.Info("field enable match, ready to create text index", zap.Int64("field id", field.GetFieldID()))

		eg.Go(func() error {
			files, err := getInsertFiles(field.GetFieldID(), field.GetNullable())
			if err != nil {
				return err
			}

			req := proto.Clone(st.req).(*workerpb.CreateStatsRequest)
			req.InsertLogs = insertBinlogs
			buildIndexParams := buildIndexParams(req, files, field, newStorageConfig, nil)

			// set analyzer extra info
			if len(analyzerExtraInfo) > 0 {
				buildIndexParams.AnalyzerExtraInfo = analyzerExtraInfo
			}

			uploaded, err := indexcgowrapper.CreateTextIndex(egCtx, buildIndexParams)
			if err != nil {
				return err
			}

			mu.Lock()
			totalSize := lo.SumBy(lo.Values(uploaded), func(fileSize int64) int64 { return fileSize })
			textIndexLogs[field.GetFieldID()] = &datapb.TextIndexStats{
				FieldID:    field.GetFieldID(),
				Version:    version,
				BuildID:    taskID,
				Files:      lo.Keys(uploaded),
				LogSize:    totalSize,
				MemorySize: totalSize,
			}
			mu.Unlock()

			log.Info("field enable match, create text index done",
				zap.Int64("targetSegmentID", st.req.GetTargetSegmentID()),
				zap.Int64("field id", field.GetFieldID()),
				zap.Strings("files", lo.Keys(uploaded)),
			)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	st.manager.StoreStatsTextIndexResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		textIndexLogs)
	totalElapse := st.tr.RecordSpan()
	log.Info("create text index done",
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Duration("total elapse", totalElapse),
	)
	return nil
}

func (st *statsTask) createJSONKeyStats(ctx context.Context,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	version int64,
	taskID int64,
	jsonKeyStatsDataFormat int64,
	insertBinlogs []*datapb.FieldBinlog,
	jsonStatsMaxShreddingColumns int64,
	jsonStatsShreddingRatioThreshold float64,
	jsonStatsWriteBatchSize int64,
) error {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("version", version),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Any("statsJobType", st.req.GetSubJobType()),
		zap.Int64("jsonKeyStatsDataFormat", jsonKeyStatsDataFormat),
		zap.Int64("jsonStatsMaxShreddingColumns", jsonStatsMaxShreddingColumns),
		zap.Float64("jsonStatsShreddingRatioThreshold", jsonStatsShreddingRatioThreshold),
		zap.Int64("jsonStatsWriteBatchSize", jsonStatsWriteBatchSize),
	)

	if jsonKeyStatsDataFormat != common.JSONStatsDataFormatVersion {
		log.Warn("create json key index failed dataformat invalid", zap.Int64("dataformat version", jsonKeyStatsDataFormat),
			zap.Int64("code version", common.JSONStatsDataFormatVersion))
		return nil
	}

	fieldBinlogs := lo.GroupBy(insertBinlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64) ([]string, error) {
		if st.req.GetStorageVersion() == storage.StorageV2 || st.req.GetStorageVersion() == storage.StorageV3 {
			return []string{}, nil
		}
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

	// Concurrent create JSON key index for all enabled fields
	var (
		mu                sync.Mutex
		jsonKeyIndexStats = make(map[int64]*datapb.JsonKeyStats)
	)

	eg, egCtx := errgroup.WithContext(ctx)

	for _, field := range st.req.GetSchema().GetFields() {
		field := field
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableJSONKeyStatsIndex() {
			continue
		}
		log.Info("field enable json key index, ready to create json key index", zap.Int64("field id", field.GetFieldID()))

		eg.Go(func() error {
			files, err := getInsertFiles(field.GetFieldID())
			if err != nil {
				return err
			}

			req := proto.Clone(st.req).(*workerpb.CreateStatsRequest)
			req.InsertLogs = insertBinlogs
			options := &BuildIndexOptions{
				JsonStatsMaxShreddingColumns: jsonStatsMaxShreddingColumns,
				JsonStatsShreddingRatio:      jsonStatsShreddingRatioThreshold,
				JsonStatsWriteBatchSize:      jsonStatsWriteBatchSize,
			}
			buildIndexParams := buildIndexParams(req, files, field, newStorageConfig, options)

			statsResult, err := indexcgowrapper.CreateJSONKeyStats(egCtx, buildIndexParams)
			if err != nil {
				return err
			}

			// calculate log size (disk size) from file sizes
			var logSize int64
			for _, fileSize := range statsResult.Files {
				logSize += fileSize
			}

			mu.Lock()
			jsonKeyIndexStats[field.GetFieldID()] = &datapb.JsonKeyStats{
				FieldID:                field.GetFieldID(),
				Version:                version,
				BuildID:                taskID,
				Files:                  lo.Keys(statsResult.Files),
				JsonKeyStatsDataFormat: jsonKeyStatsDataFormat,
				MemorySize:             statsResult.MemSize,
				LogSize:                logSize,
			}
			mu.Unlock()

			log.Info("field enable json key index, create json key index done",
				zap.Int64("field id", field.GetFieldID()),
				zap.Strings("files", lo.Keys(statsResult.Files)),
				zap.Int64("memorySize", statsResult.MemSize),
				zap.Int64("logSize", logSize),
			)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	totalElapse := st.tr.RecordSpan()

	st.manager.StoreJSONKeyStatsResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		jsonKeyIndexStats)

	metrics.DataNodeBuildJSONStatsLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(totalElapse.Seconds())
	log.Info("create json key index done",
		zap.Int64("target segmentID", st.req.GetTargetSegmentID()),
		zap.Duration("total elapse", totalElapse))
	return nil
}

func buildIndexParams(
	req *workerpb.CreateStatsRequest,
	files []string,
	field *schemapb.FieldSchema,
	storageConfig *indexcgopb.StorageConfig,
	options *BuildIndexOptions,
) *indexcgopb.BuildIndexInfo {
	if options == nil {
		options = &BuildIndexOptions{}
	}

	params := &indexcgopb.BuildIndexInfo{
		BuildID:                          req.GetTaskID(),
		CollectionID:                     req.GetCollectionID(),
		PartitionID:                      req.GetPartitionID(),
		SegmentID:                        req.GetTargetSegmentID(),
		IndexVersion:                     req.GetTaskVersion(),
		InsertFiles:                      files,
		FieldSchema:                      field,
		StorageConfig:                    storageConfig,
		CurrentScalarIndexVersion:        req.GetCurrentScalarIndexVersion(),
		StorageVersion:                   req.GetStorageVersion(),
		JsonStatsMaxShreddingColumns:     options.JsonStatsMaxShreddingColumns,
		JsonStatsShreddingRatioThreshold: options.JsonStatsShreddingRatio,
		JsonStatsWriteBatchSize:          options.JsonStatsWriteBatchSize,
		Manifest:                         req.GetManifestPath(),
	}

	if req.GetStorageVersion() == storage.StorageV2 || req.GetStorageVersion() == storage.StorageV3 {
		params.SegmentInsertFiles = util.GetSegmentInsertFiles(
			req.GetInsertLogs(),
			req.GetStorageConfig(),
			req.GetCollectionID(),
			req.GetPartitionID(),
			req.GetTargetSegmentID(),
		)
		log.Info("build index params", zap.Any("segment insert files", params.SegmentInsertFiles))
	}

	return params
}
