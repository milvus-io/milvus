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
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

// IndexBuildTask is used to record the information of the index tasks.
type indexBuildTask struct {
	ident  string
	cancel context.CancelFunc
	ctx    context.Context

	cm             storage.ChunkManager
	index          indexcgowrapper.CodecIndex
	req            *workerpb.CreateJobRequest
	newTypeParams  map[string]string
	newIndexParams map[string]string
	tr             *timerecord.TimeRecorder
	queueDur       time.Duration
	manager        *TaskManager
}

func NewIndexBuildTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.CreateJobRequest,
	cm storage.ChunkManager,
	manager *TaskManager,
) *indexBuildTask {
	t := &indexBuildTask{
		ident:   fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetBuildID()),
		cancel:  cancel,
		ctx:     ctx,
		cm:      cm,
		req:     req,
		tr:      timerecord.NewTimeRecorder(fmt.Sprintf("IndexBuildID: %d, ClusterID: %s", req.GetBuildID(), req.GetClusterID())),
		manager: manager,
	}

	t.parseParams()
	return t
}

func (it *indexBuildTask) parseParams() {
	// fill field for requests before v2.5.0
	if it.req.GetField() == nil || it.req.GetField().GetDataType() == schemapb.DataType_None || it.req.GetField().GetFieldID() == 0 {
		it.req.Field = &schemapb.FieldSchema{
			FieldID:  it.req.GetFieldID(),
			Name:     it.req.GetFieldName(),
			DataType: it.req.GetFieldType(),
		}
	}
}

func (it *indexBuildTask) Reset() {
	it.ident = ""
	it.cancel = nil
	it.ctx = nil
	it.cm = nil
	it.index = nil
	it.req = nil
	it.newTypeParams = nil
	it.newIndexParams = nil
	it.tr = nil
	it.manager = nil
}

// Ctx is the context of index tasks.
func (it *indexBuildTask) Ctx() context.Context {
	return it.ctx
}

// Name is the name of task to build index.
func (it *indexBuildTask) Name() string {
	return it.ident
}

func (it *indexBuildTask) SetState(state indexpb.JobState, failReason string) {
	it.manager.StoreIndexTaskState(it.req.GetClusterID(), it.req.GetBuildID(), commonpb.IndexState(state), failReason)
	if state == indexpb.JobState_JobStateFinished {
		metrics.DataNodeBuildIndexLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(it.tr.ElapseSpan().Seconds())
		metrics.DataNodeIndexTaskLatencyInQueue.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(it.queueDur.Milliseconds()))
	}
}

func (it *indexBuildTask) GetState() indexpb.JobState {
	return indexpb.JobState(it.manager.LoadIndexTaskState(it.req.GetClusterID(), it.req.GetBuildID()))
}

// OnEnqueue enqueues indexing tasks.
func (it *indexBuildTask) OnEnqueue(ctx context.Context) error {
	it.queueDur = 0
	it.tr.RecordSpan()
	log.Ctx(ctx).Info("IndexBuilderTask Enqueue", zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("segmentID", it.req.GetSegmentID()))
	return nil
}

func (it *indexBuildTask) GetSlot() int64 {
	return it.req.GetTaskSlot()
}

func (it *indexBuildTask) PreExecute(ctx context.Context) error {
	it.queueDur = it.tr.RecordSpan()
	log.Ctx(ctx).Info("Begin to prepare indexBuildTask", zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("Collection", it.req.GetCollectionID()), zap.Int64("SegmentID", it.req.GetSegmentID()))

	typeParams := make(map[string]string)
	indexParams := make(map[string]string)

	if len(it.req.DataPaths) == 0 {
		for _, id := range it.req.GetDataIds() {
			path := metautil.BuildInsertLogPath(it.req.GetStorageConfig().RootPath, it.req.GetCollectionID(), it.req.GetPartitionID(), it.req.GetSegmentID(), it.req.GetField().GetFieldID(), id)
			it.req.DataPaths = append(it.req.DataPaths, path)
		}
	}

	if it.req.OptionalScalarFields != nil {
		for _, optFields := range it.req.GetOptionalScalarFields() {
			if len(optFields.DataPaths) == 0 {
				for _, id := range optFields.DataIds {
					path := metautil.BuildInsertLogPath(it.req.GetStorageConfig().RootPath, it.req.GetCollectionID(), it.req.GetPartitionID(), it.req.GetSegmentID(), optFields.FieldID, id)
					optFields.DataPaths = append(optFields.DataPaths, path)
				}
			}
		}
	}

	// type params can be removed
	for _, kvPair := range it.req.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		typeParams[key] = value
		indexParams[key] = value
	}

	for _, kvPair := range it.req.GetIndexParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		// knowhere would report error if encountered the unknown key,
		// so skip this
		if key == common.MmapEnabledKey {
			continue
		}
		indexParams[key] = value
	}
	it.newTypeParams = typeParams
	it.newIndexParams = indexParams

	if it.req.GetDim() == 0 {
		// fill dim for requests before v2.4.0
		if dimStr, ok := typeParams[common.DimKey]; ok {
			var err error
			it.req.Dim, err = strconv.ParseInt(dimStr, 10, 64)
			if err != nil {
				log.Ctx(ctx).Error("parse dimesion failed", zap.Error(err))
				// ignore error
			}
		}
	}

	if it.req.GetCollectionID() == 0 || it.req.GetField().GetDataType() == schemapb.DataType_None || it.req.GetField().GetFieldID() == 0 {
		err := it.parseFieldMetaFromBinlog(ctx)
		if err != nil {
			log.Ctx(ctx).Warn("parse field meta from binlog failed", zap.Error(err))
			return err
		}
	}

	it.req.CurrentIndexVersion = getCurrentIndexVersion(it.req.GetCurrentIndexVersion())
	it.req.CurrentScalarIndexVersion = getCurrentScalarIndexVersion(it.req.GetCurrentScalarIndexVersion())

	log.Ctx(ctx).Info("Successfully prepare indexBuildTask", zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("collectionID", it.req.GetCollectionID()), zap.Int64("segmentID", it.req.GetSegmentID()),
		zap.Int64("taskVersion", it.req.GetIndexVersion()),
		zap.Int32("currentIndexVersion", it.req.GetCurrentIndexVersion()),
		zap.Int32("currentScalarIndexVersion", it.req.GetCurrentScalarIndexVersion()),
	)
	return nil
}

func (it *indexBuildTask) Execute(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", it.req.GetClusterID()), zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("collection", it.req.GetCollectionID()), zap.Int64("segmentID", it.req.GetSegmentID()),
		zap.Int32("currentIndexVersion", it.req.GetCurrentIndexVersion()))

	indexType := it.newIndexParams[common.IndexTypeKey]
	var fieldDataSize uint64
	var err error
	if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType) {
		fieldDataSize, err = estimateFieldDataSize(it.req.GetDim(), it.req.GetNumRows(), it.req.GetField().GetDataType())
		if err != nil {
			log.Warn("get local used size failed")
			return err
		}

		err = indexparams.SetDiskIndexBuildParams(it.newIndexParams, int64(fieldDataSize))
		if err != nil {
			log.Warn("failed to fill disk index params", zap.Error(err))
			return err
		}
	}

	// system resource-related parameters, such as memory limits, CPU limits, and disk limits, are appended here to the parameter list
	if vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType) && paramtable.Get().KnowhereConfig.Enable.GetAsBool() {
		it.newIndexParams, _ = paramtable.Get().KnowhereConfig.MergeResourceParams(fieldDataSize, paramtable.BuildStage, it.newIndexParams)
	}

	storageConfig := &indexcgopb.StorageConfig{
		Address:           it.req.GetStorageConfig().GetAddress(),
		AccessKeyID:       it.req.GetStorageConfig().GetAccessKeyID(),
		SecretAccessKey:   it.req.GetStorageConfig().GetSecretAccessKey(),
		UseSSL:            it.req.GetStorageConfig().GetUseSSL(),
		BucketName:        it.req.GetStorageConfig().GetBucketName(),
		RootPath:          it.req.GetStorageConfig().GetRootPath(),
		UseIAM:            it.req.GetStorageConfig().GetUseIAM(),
		IAMEndpoint:       it.req.GetStorageConfig().GetIAMEndpoint(),
		StorageType:       it.req.GetStorageConfig().GetStorageType(),
		UseVirtualHost:    it.req.GetStorageConfig().GetUseVirtualHost(),
		Region:            it.req.GetStorageConfig().GetRegion(),
		CloudProvider:     it.req.GetStorageConfig().GetCloudProvider(),
		RequestTimeoutMs:  it.req.GetStorageConfig().GetRequestTimeoutMs(),
		SslCACert:         it.req.GetStorageConfig().GetSslCACert(),
		GcpCredentialJSON: it.req.GetStorageConfig().GetGcpCredentialJSON(),
	}

	optFields := make([]*indexcgopb.OptionalFieldInfo, 0, len(it.req.GetOptionalScalarFields()))
	for _, optField := range it.req.GetOptionalScalarFields() {
		optFields = append(optFields, &indexcgopb.OptionalFieldInfo{
			FieldID:   optField.GetFieldID(),
			FieldName: optField.GetFieldName(),
			FieldType: optField.GetFieldType(),
			DataPaths: optField.GetDataPaths(),
		})
	}

	buildIndexParams := &indexcgopb.BuildIndexInfo{
		ClusterID:                 it.req.GetClusterID(),
		BuildID:                   it.req.GetBuildID(),
		CollectionID:              it.req.GetCollectionID(),
		PartitionID:               it.req.GetPartitionID(),
		SegmentID:                 it.req.GetSegmentID(),
		IndexVersion:              it.req.GetIndexVersion(),
		CurrentIndexVersion:       it.req.GetCurrentIndexVersion(),
		CurrentScalarIndexVersion: it.req.GetCurrentScalarIndexVersion(),
		NumRows:                   it.req.GetNumRows(),
		Dim:                       it.req.GetDim(),
		IndexFilePrefix:           it.req.GetIndexFilePrefix(),
		InsertFiles:               it.req.GetDataPaths(),
		FieldSchema:               it.req.GetField(),
		StorageConfig:             storageConfig,
		IndexParams:               mapToKVPairs(it.newIndexParams),
		TypeParams:                mapToKVPairs(it.newTypeParams),
		StorePath:                 it.req.GetStorePath(),
		StoreVersion:              it.req.GetStoreVersion(),
		IndexStorePath:            it.req.GetIndexStorePath(),
		OptFields:                 optFields,
		PartitionKeyIsolation:     it.req.GetPartitionKeyIsolation(),
		LackBinlogRows:            it.req.GetLackBinlogRows(),
		StorageVersion:            it.req.GetStorageVersion(),
	}

	if buildIndexParams.StorageVersion == storage.StorageV2 {
		buildIndexParams.SegmentInsertFiles = GetSegmentInsertFiles(
			it.req.GetInsertLogs(),
			it.req.GetStorageConfig(),
			it.req.GetCollectionID(),
			it.req.GetPartitionID(),
			it.req.GetSegmentID())
	}

	log.Info("create index", zap.Any("buildIndexParams", buildIndexParams))

	it.index, err = indexcgowrapper.CreateIndex(ctx, buildIndexParams)
	if err != nil {
		if it.index != nil && it.index.CleanLocalData() != nil {
			log.Warn("failed to clean cached data on disk after build index failed")
		}
		log.Warn("failed to build index", zap.Error(err))
		return err
	}

	buildIndexLatency := it.tr.RecordSpan()
	metrics.DataNodeKnowhereBuildIndexLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(buildIndexLatency.Seconds())

	log.Info("Successfully build index")
	return nil
}

func (it *indexBuildTask) PostExecute(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", it.req.GetClusterID()), zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("collection", it.req.GetCollectionID()), zap.Int64("segmentID", it.req.GetSegmentID()),
		zap.Int32("currentIndexVersion", it.req.GetCurrentIndexVersion()))

	gcIndex := func() {
		if err := it.index.Delete(); err != nil {
			log.Warn("indexBuildTask Execute CIndexDelete failed", zap.Error(err))
		}
	}
	indexStats, err := it.index.UpLoad()
	if err != nil {
		log.Warn("failed to upload index", zap.Error(err))
		gcIndex()
		return err
	}
	encodeIndexFileDur := it.tr.Record("index serialize and upload done")
	metrics.DataNodeEncodeIndexFileLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(encodeIndexFileDur.Seconds())

	// early release index for gc, and we can ensure that Delete is idempotent.
	gcIndex()

	// use serialized size before encoding
	var serializedSize uint64
	saveFileKeys := make([]string, 0)
	for _, indexInfo := range indexStats.GetSerializedIndexInfos() {
		serializedSize += uint64(indexInfo.FileSize)
		parts := strings.Split(indexInfo.FileName, "/")
		fileKey := parts[len(parts)-1]
		saveFileKeys = append(saveFileKeys, fileKey)
	}

	it.manager.StoreIndexFilesAndStatistic(
		it.req.GetClusterID(),
		it.req.GetBuildID(),
		saveFileKeys,
		serializedSize,
		uint64(indexStats.MemSize),
		it.req.GetCurrentIndexVersion(),
		it.req.GetCurrentScalarIndexVersion(),
	)
	saveIndexFileDur := it.tr.RecordSpan()
	metrics.DataNodeSaveIndexFileLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(saveIndexFileDur.Seconds())
	it.tr.Elapse("index building all done")
	log.Info("Successfully save index files",
		zap.Uint64("serializedSize", serializedSize),
		zap.Int64("memSize", indexStats.MemSize),
		zap.Strings("indexFiles", saveFileKeys))
	return nil
}

func (it *indexBuildTask) parseFieldMetaFromBinlog(ctx context.Context) error {
	// fill collectionID, partitionID... for requests before v2.4.0
	toLoadDataPaths := it.req.GetDataPaths()
	if len(toLoadDataPaths) == 0 {
		return merr.WrapErrParameterInvalidMsg("data insert path must be not empty")
	}
	data, err := it.cm.Read(ctx, toLoadDataPaths[0])
	if err != nil {
		return err
	}

	var insertCodec storage.InsertCodec
	collectionID, partitionID, segmentID, insertData, err := insertCodec.DeserializeAll([]*storage.Blob{{Key: toLoadDataPaths[0], Value: data}})
	if err != nil {
		return err
	}
	if len(insertData.Data) != 1 {
		return merr.WrapErrParameterInvalidMsg("we expect only one field in deserialized insert data")
	}

	it.req.CollectionID = collectionID
	it.req.PartitionID = partitionID
	it.req.SegmentID = segmentID
	if it.req.GetField().GetDataType() == schemapb.DataType_None || it.req.GetField().GetFieldID() == 0 {
		for fID, value := range insertData.Data {
			it.req.Field.DataType = value.GetDataType()
			it.req.Field.FieldID = fID
			break
		}
	}

	return nil
}
