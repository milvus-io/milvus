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
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzecgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

var _ Task = (*analyzeTask)(nil)

type analyzeTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *workerpb.AnalyzeRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	manager  *TaskManager
	analyze  analyzecgowrapper.CodecAnalyze

	pluginContext *indexcgopb.StoragePluginContext
}

func NewAnalyzeTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.AnalyzeRequest,
	manager *TaskManager,
	pluginContext *indexcgopb.StoragePluginContext,
) *analyzeTask {
	return &analyzeTask{
		ident:         fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetTaskID()),
		ctx:           ctx,
		cancel:        cancel,
		req:           req,
		manager:       manager,
		pluginContext: pluginContext,
		tr:            timerecord.NewTimeRecorder(fmt.Sprintf("ClusterID: %s, TaskID: %d", req.GetClusterID(), req.GetTaskID())),
	}
}

func (at *analyzeTask) Ctx() context.Context {
	return at.ctx
}

func (at *analyzeTask) Name() string {
	return at.ident
}

func (at *analyzeTask) GetSlot() int64 {
	return at.req.GetTaskSlot()
}

func (at *analyzeTask) GetResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: at.req.GetCpu(), Memory: at.req.GetMemory()}
}

func (at *analyzeTask) IsVectorIndex() bool {
	return false
}

func (at *analyzeTask) PreExecute(ctx context.Context) error {
	at.queueDur = at.tr.RecordSpan()
	log := mlog.With(mlog.String("clusterID", at.req.GetClusterID()),
		mlog.Int64("TaskID", at.req.GetTaskID()), mlog.Int64("Collection", at.req.GetCollectionID()),
		mlog.FieldPartitionID(at.req.GetPartitionID()), mlog.FieldFieldID(at.req.GetFieldID()))
	log.Info(ctx, "Begin to prepare analyze task")

	log.Info(ctx, "Successfully prepare analyze task, nothing to do...")
	return nil
}

func buildAnalyzeInfo(req *workerpb.AnalyzeRequest) *clusteringpb.AnalyzeInfo {
	storageConfig := &clusteringpb.StorageConfig{
		Address:           req.GetStorageConfig().GetAddress(),
		AccessKeyID:       req.GetStorageConfig().GetAccessKeyID(),
		SecretAccessKey:   req.GetStorageConfig().GetSecretAccessKey(),
		UseSSL:            req.GetStorageConfig().GetUseSSL(),
		BucketName:        req.GetStorageConfig().GetBucketName(),
		RootPath:          req.GetStorageConfig().GetRootPath(),
		UseIAM:            req.GetStorageConfig().GetUseIAM(),
		IAMEndpoint:       req.GetStorageConfig().GetIAMEndpoint(),
		StorageType:       req.GetStorageConfig().GetStorageType(),
		UseVirtualHost:    req.GetStorageConfig().GetUseVirtualHost(),
		Region:            req.GetStorageConfig().GetRegion(),
		CloudProvider:     req.GetStorageConfig().GetCloudProvider(),
		RequestTimeoutMs:  req.GetStorageConfig().GetRequestTimeoutMs(),
		MaxConnections:    req.GetStorageConfig().GetMaxConnections(),
		SslCACert:         req.GetStorageConfig().GetSslCACert(),
		GcpCredentialJSON: req.GetStorageConfig().GetGcpCredentialJSON(),
		SslTlsMinVersion:  req.GetStorageConfig().GetSslTlsMinVersion(),
		UseCrc32CChecksum: req.GetStorageConfig().GetUseCrc32CChecksum(),
	}

	n := len(req.GetSegmentStats())
	numRowsMap := make(map[int64]int64, n)
	segmentInsertFilesMap := make(map[int64]*clusteringpb.InsertFiles, n)
	manifestPathsMap := make(map[int64]string, n)

	for segID, stats := range req.GetSegmentStats() {
		numRowsMap[segID] = stats.GetNumRows()

		if manifest := stats.GetManifestPath(); manifest != "" {
			// StorageV3: forward the manifest string; C++ resolves files via loon.
			manifestPathsMap[segID] = manifest
			continue
		}

		// V1: reconstruct insert-log paths from logIDs.
		insertFiles := make([]string, 0, len(stats.GetLogIDs()))
		for _, id := range stats.GetLogIDs() {
			path := metautil.BuildInsertLogPath(req.GetStorageConfig().RootPath,
				req.GetCollectionID(), req.GetPartitionID(), segID, req.GetFieldID(), id)
			insertFiles = append(insertFiles, path)
		}
		segmentInsertFilesMap[segID] = &clusteringpb.InsertFiles{InsertFiles: insertFiles}
	}

	field := req.GetField()
	if field == nil || field.GetDataType() == schemapb.DataType_None {
		field = &schemapb.FieldSchema{
			FieldID:  req.GetFieldID(),
			Name:     req.GetFieldName(),
			DataType: req.GetFieldType(),
		}
	}

	return &clusteringpb.AnalyzeInfo{
		ClusterID:       req.GetClusterID(),
		BuildID:         req.GetTaskID(),
		CollectionID:    req.GetCollectionID(),
		PartitionID:     req.GetPartitionID(),
		Version:         req.GetVersion(),
		Dim:             req.GetDim(),
		StorageConfig:   storageConfig,
		NumClusters:     req.GetNumClusters(),
		TrainSize:       int64(float64(hardware.GetMemoryCount()) * req.GetMaxTrainSizeRatio()),
		MinClusterRatio: req.GetMinClusterSizeRatio(),
		MaxClusterRatio: req.GetMaxClusterSizeRatio(),
		MaxClusterSize:  req.GetMaxClusterSize(),
		NumRows:         numRowsMap,
		InsertFiles:     segmentInsertFilesMap,
		FieldSchema:     field,
		ManifestPaths:   manifestPathsMap,
	}
}

func (at *analyzeTask) Execute(ctx context.Context) error {
	var err error

	log := mlog.With(mlog.String("clusterID", at.req.GetClusterID()),
		mlog.Int64("TaskID", at.req.GetTaskID()), mlog.Int64("Collection", at.req.GetCollectionID()),
		mlog.FieldPartitionID(at.req.GetPartitionID()), mlog.FieldFieldID(at.req.GetFieldID()))

	log.Info(ctx, "Begin to build analyze task")

	analyzeInfo := buildAnalyzeInfo(at.req)

	at.analyze, err = analyzecgowrapper.Analyze(ctx, analyzeInfo, at.pluginContext)
	if err != nil {
		log.Error(ctx, "failed to analyze data", mlog.Err(err))
		return err
	}

	analyzeLatency := at.tr.RecordSpan()
	log.Info(ctx, "analyze done", mlog.Int64("analyze cost", analyzeLatency.Milliseconds()))
	return nil
}

func (at *analyzeTask) PostExecute(ctx context.Context) error {
	log := mlog.With(mlog.String("clusterID", at.req.GetClusterID()),
		mlog.Int64("TaskID", at.req.GetTaskID()), mlog.Int64("Collection", at.req.GetCollectionID()),
		mlog.FieldPartitionID(at.req.GetPartitionID()), mlog.FieldFieldID(at.req.GetFieldID()))
	gc := func() {
		if err := at.analyze.Delete(); err != nil {
			log.Error(ctx, "indexBuildTask Execute CIndexDelete failed", mlog.Err(err))
		}
	}
	defer gc()

	centroidsFile, _, _, _, err := at.analyze.GetResult(len(at.req.GetSegmentStats()))
	if err != nil {
		log.Error(ctx, "failed to upload index", mlog.Err(err))
		return err
	}
	log.Info(ctx, "analyze result", mlog.String("centroidsFile", centroidsFile))

	at.manager.StoreAnalyzeFilesAndStatistic(at.req.GetClusterID(),
		at.req.GetTaskID(),
		centroidsFile)
	at.tr.Elapse("index building all done")
	log.Info(ctx, "Successfully save analyze files")
	return nil
}

func (at *analyzeTask) OnEnqueue(ctx context.Context) error {
	at.queueDur = 0
	at.tr.RecordSpan()

	mlog.Info(ctx, "analyzeTask enqueued", mlog.String("clusterID", at.req.GetClusterID()),
		mlog.Int64("TaskID", at.req.GetTaskID()))
	return nil
}

func (at *analyzeTask) SetState(state indexpb.JobState, failReason string) {
	at.manager.StoreAnalyzeTaskState(at.req.GetClusterID(), at.req.GetTaskID(), state, failReason)
}

func (at *analyzeTask) GetState() indexpb.JobState {
	return at.manager.LoadAnalyzeTaskState(at.req.GetClusterID(), at.req.GetTaskID())
}

func (at *analyzeTask) Reset() {
	at.ident = ""
	at.ctx = nil
	at.cancel = nil
	at.req = nil
	at.tr = nil
	at.queueDur = 0
	at.manager = nil
	at.pluginContext = nil
}
