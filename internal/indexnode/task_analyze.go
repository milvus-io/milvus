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
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/analyzecgowrapper"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type analyzeTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *indexpb.AnalyzeRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	node     *IndexNode
	analyze  analyzecgowrapper.CodecAnalyze

	startTime int64
	endTime   int64
}

func (at *analyzeTask) Ctx() context.Context {
	return at.ctx
}

func (at *analyzeTask) Name() string {
	return at.ident
}

func (at *analyzeTask) Prepare(ctx context.Context) error {
	at.queueDur = at.tr.RecordSpan()
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))
	log.Info("Begin to prepare analyze task")

	log.Info("Successfully prepare analyze task, nothing to do...")
	return nil
}

func (at *analyzeTask) LoadData(ctx context.Context) error {
	// Load data in segcore
	return nil
}

func (at *analyzeTask) BuildIndex(ctx context.Context) error {
	var err error
	var analyzeInfo *analyzecgowrapper.AnalyzeInfo
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))

	log.Info("Begin to build analyze task")
	analyzeInfo, err = analyzecgowrapper.NewAnalyzeInfo(at.req.GetStorageConfig())
	defer analyzecgowrapper.DeleteAnalyzeInfo(analyzeInfo)
	if err != nil {
		log.Warn("create analyze info failed", zap.Error(err))
		return err
	}

	for segID, stats := range at.req.GetSegmentStats() {
		numRows := stats.GetNumRows()
		err = analyzeInfo.AppendNumRows(segID, numRows)
		log.Info("append segment rows", zap.Int64("segment id", segID), zap.Int64("rows", numRows))
		if err != nil {
			log.Warn("append segment num rows failed", zap.Error(err))
			return err
		}
		for _, id := range stats.GetLogIDs() {
			path := metautil.BuildInsertLogPath(at.req.GetStorageConfig().RootPath,
				at.req.GetCollectionID(), at.req.GetPartitionID(), segID, at.req.GetFieldID(), id)
			err = analyzeInfo.AppendSegmentInsertFile(segID, path)
			if err != nil {
				log.Warn("append insert binlog path failed", zap.Error(err))
				return err
			}
		}
	}

	err = analyzeInfo.AppendAnalyzeInfo(
		at.req.GetCollectionID(),
		at.req.GetPartitionID(),
		at.req.GetFieldID(),
		at.req.GetTaskID(),
		at.req.GetVersion(),
		at.req.GetFieldName(),
		at.req.GetFieldType(),
		at.req.GetDim(),
		at.req.GetNumClusters(),
		at.req.GetMaxTrainSize(),
	)
	if err != nil {
		log.Warn("append analyze info failed", zap.Error(err))
		return err
	}

	at.analyze, err = analyzecgowrapper.Analyze(ctx, analyzeInfo)
	if err != nil {
		log.Error("failed to analyze data", zap.Error(err))
		return err
	}

	analyzeLatency := at.tr.RecordSpan()
	log.Info("analyze done", zap.Int64("analyze cost", analyzeLatency.Milliseconds()))
	return nil
}

func (at *analyzeTask) SaveResult(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))
	gc := func() {
		if err := at.analyze.Delete(); err != nil {
			log.Error("IndexNode indexBuildTask Execute CIndexDelete failed", zap.Error(err))
		}
	}
	defer gc()

	centroidsFile, centroidsFileSize, offsetMappingFiles, offsetMappingFilesSize, err := at.analyze.GetResult(len(at.req.GetSegmentStats()))
	if err != nil {
		log.Error("failed to upload index", zap.Error(err))
		return err
	}
	log.Info("debug for analyze result", zap.String("centroidsFile", centroidsFile),
		zap.Int64("centroidsFileSize", centroidsFileSize))
	segmentsOffsetMappingFiles := make(map[int64]string)
	segmentsOffsetMappingFilesSize := make(map[string]int64)
	for i, file := range offsetMappingFiles {
		for segID := range at.req.GetSegmentStats() {
			if strings.Contains(file, fmt.Sprintf("%d", segID)) {
				segmentsOffsetMappingFiles[segID] = file
				segmentsOffsetMappingFilesSize[file] = offsetMappingFilesSize[i]
				log.Info("debug for analyze result", zap.Int64("segID", segID),
					zap.String("offsetMappingFile", file),
					zap.Int64("offsetMappingFileSize", offsetMappingFilesSize[i]))
				break
			}
		}
	}

	at.endTime = time.Now().UnixMicro()
	at.node.storeAnalyzeFilesAndStatistic(at.req.GetClusterID(),
		at.req.GetTaskID(),
		centroidsFile,
		segmentsOffsetMappingFiles,
		centroidsFileSize,
		segmentsOffsetMappingFilesSize)
	at.tr.Elapse("index building all done")
	log.Info("Successfully save analyze files")
	return nil
}

func (at *analyzeTask) OnEnqueue(ctx context.Context) error {
	at.queueDur = 0
	at.tr.RecordSpan()
	at.startTime = time.Now().UnixMicro()
	log.Ctx(ctx).Info("IndexNode analyzeTask enqueued", zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()))
	return nil
}

func (at *analyzeTask) SetState(state indexpb.JobState, failReason string) {
	at.node.storeAnalyzeTaskState(at.req.GetClusterID(), at.req.GetTaskID(), state, failReason)
}

func (at *analyzeTask) GetState() indexpb.JobState {
	return at.node.loadAnalyzeTaskState(at.req.GetClusterID(), at.req.GetTaskID())
}

func (at *analyzeTask) Reset() {
	at.ident = ""
	at.ctx = nil
	at.cancel = nil
	at.req = nil
	at.tr = nil
	at.queueDur = 0
	at.node = nil
	at.startTime = 0
	at.endTime = 0
}
