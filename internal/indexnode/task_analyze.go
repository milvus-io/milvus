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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/clusteringpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/util/analyzecgowrapper"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var _ task = (*analyzeTask)(nil)

type analyzeTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *workerpb.AnalyzeRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	node     *IndexNode
	analyze  analyzecgowrapper.CodecAnalyze
}

func newAnalyzeTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.AnalyzeRequest,
	node *IndexNode,
) *analyzeTask {
	return &analyzeTask{
		ident:  fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetTaskID()),
		ctx:    ctx,
		cancel: cancel,
		req:    req,
		node:   node,
		tr:     timerecord.NewTimeRecorder(fmt.Sprintf("ClusterID: %s, TaskID: %d", req.GetClusterID(), req.GetTaskID())),
	}
}

func (at *analyzeTask) Ctx() context.Context {
	return at.ctx
}

func (at *analyzeTask) Name() string {
	return at.ident
}

func (at *analyzeTask) PreExecute(ctx context.Context) error {
	at.queueDur = at.tr.RecordSpan()
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("TaskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))
	log.Info("Begin to prepare analyze task")

	log.Info("Successfully prepare analyze task, nothing to do...")
	return nil
}

func (at *analyzeTask) Execute(ctx context.Context) error {
	var err error

	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("TaskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))

	log.Info("Begin to build analyze task")

	storageConfig := &clusteringpb.StorageConfig{
		Address:          at.req.GetStorageConfig().GetAddress(),
		AccessKeyID:      at.req.GetStorageConfig().GetAccessKeyID(),
		SecretAccessKey:  at.req.GetStorageConfig().GetSecretAccessKey(),
		UseSSL:           at.req.GetStorageConfig().GetUseSSL(),
		BucketName:       at.req.GetStorageConfig().GetBucketName(),
		RootPath:         at.req.GetStorageConfig().GetRootPath(),
		UseIAM:           at.req.GetStorageConfig().GetUseIAM(),
		IAMEndpoint:      at.req.GetStorageConfig().GetIAMEndpoint(),
		StorageType:      at.req.GetStorageConfig().GetStorageType(),
		UseVirtualHost:   at.req.GetStorageConfig().GetUseVirtualHost(),
		Region:           at.req.GetStorageConfig().GetRegion(),
		CloudProvider:    at.req.GetStorageConfig().GetCloudProvider(),
		RequestTimeoutMs: at.req.GetStorageConfig().GetRequestTimeoutMs(),
		SslCACert:        at.req.GetStorageConfig().GetSslCACert(),
	}

	numRowsMap := make(map[int64]int64)
	segmentInsertFilesMap := make(map[int64]*clusteringpb.InsertFiles)

	for segID, stats := range at.req.GetSegmentStats() {
		numRows := stats.GetNumRows()
		numRowsMap[segID] = numRows
		log.Info("append segment rows", zap.Int64("segment id", segID), zap.Int64("rows", numRows))
		insertFiles := make([]string, 0, len(stats.GetLogIDs()))
		for _, id := range stats.GetLogIDs() {
			path := metautil.BuildInsertLogPath(at.req.GetStorageConfig().RootPath,
				at.req.GetCollectionID(), at.req.GetPartitionID(), segID, at.req.GetFieldID(), id)
			insertFiles = append(insertFiles, path)
		}
		segmentInsertFilesMap[segID] = &clusteringpb.InsertFiles{InsertFiles: insertFiles}
	}

	field := at.req.GetField()
	if field == nil || field.GetDataType() == schemapb.DataType_None {
		field = &schemapb.FieldSchema{
			FieldID:  at.req.GetFieldID(),
			Name:     at.req.GetFieldName(),
			DataType: at.req.GetFieldType(),
		}
	}

	analyzeInfo := &clusteringpb.AnalyzeInfo{
		ClusterID:       at.req.GetClusterID(),
		BuildID:         at.req.GetTaskID(),
		CollectionID:    at.req.GetCollectionID(),
		PartitionID:     at.req.GetPartitionID(),
		Version:         at.req.GetVersion(),
		Dim:             at.req.GetDim(),
		StorageConfig:   storageConfig,
		NumClusters:     at.req.GetNumClusters(),
		TrainSize:       int64(float64(hardware.GetMemoryCount()) * at.req.GetMaxTrainSizeRatio()),
		MinClusterRatio: at.req.GetMinClusterSizeRatio(),
		MaxClusterRatio: at.req.GetMaxClusterSizeRatio(),
		MaxClusterSize:  at.req.GetMaxClusterSize(),
		NumRows:         numRowsMap,
		InsertFiles:     segmentInsertFilesMap,
		FieldSchema:     field,
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

func (at *analyzeTask) PostExecute(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("TaskID", at.req.GetTaskID()), zap.Int64("Collection", at.req.GetCollectionID()),
		zap.Int64("partitionID", at.req.GetPartitionID()), zap.Int64("fieldID", at.req.GetFieldID()))
	gc := func() {
		if err := at.analyze.Delete(); err != nil {
			log.Error("IndexNode indexBuildTask Execute CIndexDelete failed", zap.Error(err))
		}
	}
	defer gc()

	centroidsFile, _, _, _, err := at.analyze.GetResult(len(at.req.GetSegmentStats()))
	if err != nil {
		log.Error("failed to upload index", zap.Error(err))
		return err
	}
	log.Info("analyze result", zap.String("centroidsFile", centroidsFile))

	at.node.storeAnalyzeFilesAndStatistic(at.req.GetClusterID(),
		at.req.GetTaskID(),
		centroidsFile)
	at.tr.Elapse("index building all done")
	log.Info("Successfully save analyze files")
	return nil
}

func (at *analyzeTask) OnEnqueue(ctx context.Context) error {
	at.queueDur = 0
	at.tr.RecordSpan()

	log.Ctx(ctx).Info("IndexNode analyzeTask enqueued", zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("TaskID", at.req.GetTaskID()))
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
}
