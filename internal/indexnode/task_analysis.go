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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/analysiscgowrapper"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type analysisTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *indexpb.AnalysisRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	node     *IndexNode
	analysis analysiscgowrapper.CodecAnalysis

	segmentIDs []int64
	dataPaths  map[int64][]string
	startTime  int64
	endTime    int64
}

func (at *analysisTask) Ctx() context.Context {
	return at.ctx
}
func (at *analysisTask) Name() string {
	return at.ident
}

func (at *analysisTask) Prepare(ctx context.Context) error {
	at.queueDur = at.tr.RecordSpan()
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))
	log.Info("Begin to prepare analysis task")

	at.segmentIDs = make([]int64, 0)
	at.dataPaths = make(map[int64][]string)
	for segID, stats := range at.req.GetSegmentStats() {
		at.segmentIDs = append(at.segmentIDs, segID)
		at.dataPaths[segID] = make([]string, 0)
		for _, id := range stats.GetLogIDs() {
			path := metautil.BuildInsertLogPath(at.req.GetStorageConfig().RootPath,
				at.req.GetCollectionID(), at.req.GetPartitionID(), segID, at.req.GetFieldID(), id)
			at.dataPaths[segID] = append(at.dataPaths[segID], path)
		}
	}

	log.Info("Successfully prepare analysis task", zap.Any("dataPaths", at.dataPaths))
	return nil
}

func (at *analysisTask) LoadData(ctx context.Context) error {
	// Load data in segcore
	return nil
}
func (at *analysisTask) BuildIndex(ctx context.Context) error {
	var err error
	var analysisInfo *analysiscgowrapper.AnalysisInfo
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))

	log.Info("Begin to build analysis task")
	analysisInfo, err = analysiscgowrapper.NewAnalysisInfo(at.req.GetStorageConfig())
	defer analysiscgowrapper.DeleteAnalysisInfo(analysisInfo)
	if err != nil {
		log.Warn("create analysis info failed", zap.Error(err))
		return err
	}
	err = analysisInfo.AppendAnalysisFieldMetaInfo(at.req.GetCollectionID(), at.req.GetPartitionID(),
		at.req.GetFieldID(), at.req.GetFieldType(), at.req.GetFieldName(), at.req.GetDim())
	if err != nil {
		log.Warn("append field meta failed", zap.Error(err))
		return err
	}

	err = analysisInfo.AppendAnalysisInfo(at.req.GetTaskID(), at.req.GetVersion())
	if err != nil {
		log.Warn("append index meta failed", zap.Error(err))
		return err
	}

	for segID, paths := range at.dataPaths {
		for _, path := range paths {
			err = analysisInfo.AppendSegmentInsertFile(segID, path)
			if err != nil {
				log.Warn("append insert binlog path failed", zap.Error(err))
				return err
			}
		}
	}

	err = analysisInfo.AppendSegmentSize(Params.DataCoordCfg.ClusteringCompactionPreferSegmentSize.GetAsSize())
	if err != nil {
		log.Warn("append segment size failed", zap.Error(err))
		return err
	}

	err = analysisInfo.AppendTrainSize(Params.DataCoordCfg.ClusteringCompactionMaxTrainSize.GetAsSize())
	if err != nil {
		log.Warn("append train size failed", zap.Error(err))
		return err
	}

	at.analysis, err = analysiscgowrapper.Analysis(ctx, analysisInfo)
	if err != nil {
		if at.analysis != nil && at.analysis.CleanLocalData() != nil {
			log.Error("failed to clean cached data on disk after analysis failed",
				zap.Int64("buildID", at.req.GetTaskID()),
				zap.Int64("index version", at.req.GetVersion()))
		}
		log.Error("failed to analysis data", zap.Error(err))
		return err
	}

	analysisLatency := at.tr.RecordSpan()
	log.Info("analysis done", zap.Int64("analysis cost", analysisLatency.Milliseconds()))
	return nil
}

func (at *analysisTask) SaveIndexFiles(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))
	gc := func() {
		if err := at.analysis.Delete(); err != nil {
			log.Error("IndexNode indexBuildTask Execute CIndexDelete failed", zap.Error(err))
		}
	}
	centroidsFile, segmentsOffsetMapping, err := at.analysis.UpLoad(at.segmentIDs)
	if err != nil {
		log.Error("failed to upload index", zap.Error(err))
		gc()
		return err
	}
	//encodeIndexFileDur := at.tr.Record("index serialize and upload done")
	//metrics.IndexNodeEncodeIndexFileLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(encodeIndexFileDur.Seconds())

	// early release analysis for gc, and we can ensure that Delete is idempotent.
	gc()

	at.endTime = time.Now().UnixMicro()
	at.node.storeAnalysisStatistic(at.req.GetClusterID(), at.req.GetTaskID(), centroidsFile, segmentsOffsetMapping)
	//saveIndexFileDur := at.tr.RecordSpan()
	//metrics.IndexNodeSaveIndexFileLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(saveIndexFileDur.Seconds())
	at.tr.Elapse("index building all done")
	log.Info("Successfully save analysis files")
	return nil
}

func (at *analysisTask) OnEnqueue(ctx context.Context) error {
	at.queueDur = 0
	at.tr.RecordSpan()
	at.startTime = time.Now().UnixMicro()
	log.Ctx(ctx).Info("IndexNode analysisTask enqueued", zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()))
	return nil
}

func (at *analysisTask) SetState(state commonpb.IndexState, failReason string) {
	at.node.storeAnalysisTaskState(at.req.GetClusterID(), at.req.GetTaskID(), state, failReason)
}

func (at *analysisTask) GetState() commonpb.IndexState {
	return at.node.loadAnalysisTaskState(at.req.GetClusterID(), at.req.GetTaskID())
}

func (at *analysisTask) Reset() {
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
