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
	"strconv"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
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

	currentTime time.Time
}

func NewStatsTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.CreateStatsRequest,
	manager *TaskManager,
) *statsTask {
	return &statsTask{
		ident:       fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetTaskID()),
		ctx:         ctx,
		cancel:      cancel,
		req:         req,
		manager:     manager,
		tr:          timerecord.NewTimeRecorder(fmt.Sprintf("ClusterID: %s, TaskID: %d", req.GetClusterID(), req.GetTaskID())),
		currentTime: tsoutil.PhysicalTime(req.GetCurrentTs()),
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

	preExecuteRecordSpan := st.tr.RecordSpan()

	log.Ctx(ctx).Info("successfully PreExecute stats task",
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Int64("preExecuteRecordSpan(ms)", preExecuteRecordSpan.Milliseconds()),
		zap.Any("storageConfig", st.req.StorageConfig),
	)
	return nil
}

func (st *statsTask) Execute(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-Execute-%s-%d", st.req.GetClusterID(), st.req.GetTaskID()))
	defer span.End()

	insertLogs := st.req.GetInsertLogs()
	var err error
	if len(insertLogs) == 0 {
		log.Ctx(ctx).Info("there is no insertBinlogs, skip creating text index")
		return nil
	}

	if st.req.GetSubJobType() == indexpb.StatsSubJob_TextIndexJob {
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
	if st.req.GetSubJobType() == indexpb.StatsSubJob_JsonKeyIndexJob {
		if !st.req.GetEnableJsonKeyStats() {
			return nil
		}

		err = st.createJSONKeyStats(ctx,
			st.req.GetStorageConfig(),
			st.req.GetCollectionID(),
			st.req.GetPartitionID(),
			st.req.GetTargetSegmentID(),
			st.req.GetTaskVersion(),
			st.req.GetTaskID(),
			st.req.GetJsonKeyStatsTantivyMemory(),
			st.req.GetJsonKeyStatsDataFormat(),
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
	st.manager = nil
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

	newStorageConfig, err := util.ParseStorageConfig(storageConfig)
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

		buildIndexParams := buildIndexParams(st.req, files, field, newStorageConfig, 0)

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

	st.manager.StoreStatsTextIndexResult(st.req.GetClusterID(),
		st.req.GetTaskID(),
		st.req.GetCollectionID(),
		st.req.GetPartitionID(),
		st.req.GetTargetSegmentID(),
		st.req.GetInsertChannel(),
		textIndexLogs)
	return nil
}

func (st *statsTask) createJSONKeyStats(ctx context.Context,
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	version int64,
	taskID int64,
	tantivyMemory int64,
	jsonKeyStatsDataFormat int64,
	insertBinlogs []*datapb.FieldBinlog,
) error {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", st.req.GetClusterID()),
		zap.Int64("taskID", st.req.GetTaskID()),
		zap.Int64("collectionID", st.req.GetCollectionID()),
		zap.Int64("partitionID", st.req.GetPartitionID()),
		zap.Int64("segmentID", st.req.GetSegmentID()),
		zap.Any("statsJobType", st.req.GetSubJobType()),
		zap.Int64("jsonKeyStatsDataFormat", jsonKeyStatsDataFormat),
	)
	if jsonKeyStatsDataFormat != 1 {
		log.Info("create json key index failed dataformat invalid")
		return nil
	}
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

	newStorageConfig, err := util.ParseStorageConfig(storageConfig)
	if err != nil {
		return err
	}

	jsonKeyIndexStats := make(map[int64]*datapb.JsonKeyStats)
	for _, field := range st.req.GetSchema().GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableJSONKeyStatsIndex() {
			continue
		}
		log.Info("field enable json key index, ready to create json key index", zap.Int64("field id", field.GetFieldID()))
		files, err := getInsertFiles(field.GetFieldID())
		if err != nil {
			return err
		}

		buildIndexParams := buildIndexParams(st.req, files, field, newStorageConfig, tantivyMemory)

		uploaded, err := indexcgowrapper.CreateJSONKeyStats(ctx, buildIndexParams)
		if err != nil {
			return err
		}
		jsonKeyIndexStats[field.GetFieldID()] = &datapb.JsonKeyStats{
			FieldID:                field.GetFieldID(),
			Version:                version,
			BuildID:                taskID,
			Files:                  lo.Keys(uploaded),
			JsonKeyStatsDataFormat: jsonKeyStatsDataFormat,
		}
		log.Info("field enable json key index, create json key index done",
			zap.Int64("field id", field.GetFieldID()),
			zap.Strings("files", lo.Keys(uploaded)),
		)
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
	tantivyMemory int64,
) *indexcgopb.BuildIndexInfo {
	params := &indexcgopb.BuildIndexInfo{
		BuildID:                   req.GetTaskID(),
		CollectionID:              req.GetCollectionID(),
		PartitionID:               req.GetPartitionID(),
		SegmentID:                 req.GetTargetSegmentID(),
		IndexVersion:              req.GetTaskVersion(),
		InsertFiles:               files,
		FieldSchema:               field,
		StorageConfig:             storageConfig,
		CurrentScalarIndexVersion: req.GetCurrentScalarIndexVersion(),
		StorageVersion:            req.GetStorageVersion(),
		JsonKeyStatsTantivyMemory: tantivyMemory,
	}

	if req.GetStorageVersion() == storage.StorageV2 {
		params.SegmentInsertFiles = util.GetSegmentInsertFiles(
			req.GetInsertLogs(),
			req.GetStorageConfig(),
			req.GetCollectionID(),
			req.GetPartitionID(),
			req.GetTargetSegmentID())
	}

	return params
}
