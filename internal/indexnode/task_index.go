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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
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
	node           *IndexNode
}

func newIndexBuildTask(ctx context.Context,
	cancel context.CancelFunc,
	req *workerpb.CreateJobRequest,
	cm storage.ChunkManager,
	node *IndexNode,
) *indexBuildTask {
	t := &indexBuildTask{
		ident:  fmt.Sprintf("%s/%d", req.GetClusterID(), req.GetBuildID()),
		cancel: cancel,
		ctx:    ctx,
		cm:     cm,
		req:    req,
		tr:     timerecord.NewTimeRecorder(fmt.Sprintf("IndexBuildID: %d, ClusterID: %s", req.GetBuildID(), req.GetClusterID())),
		node:   node,
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
	it.node = nil
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
	it.node.storeIndexTaskState(it.req.GetClusterID(), it.req.GetBuildID(), commonpb.IndexState(state), failReason)
}

func (it *indexBuildTask) GetState() indexpb.JobState {
	return indexpb.JobState(it.node.loadIndexTaskState(it.req.GetClusterID(), it.req.GetBuildID()))
}

// OnEnqueue enqueues indexing tasks.
func (it *indexBuildTask) OnEnqueue(ctx context.Context) error {
	it.queueDur = 0
	it.tr.RecordSpan()
	log.Ctx(ctx).Info("IndexNode IndexBuilderTask Enqueue", zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("segmentID", it.req.GetSegmentID()))
	return nil
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

	log.Ctx(ctx).Info("Successfully prepare indexBuildTask", zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("collectionID", it.req.GetCollectionID()), zap.Int64("segmentID", it.req.GetSegmentID()),
		zap.Int64("currentIndexVersion", it.req.GetIndexVersion()))
	return nil
}

func (it *indexBuildTask) Execute(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", it.req.GetClusterID()), zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("collection", it.req.GetCollectionID()), zap.Int64("segmentID", it.req.GetSegmentID()),
		zap.Int32("currentIndexVersion", it.req.GetCurrentIndexVersion()))

	indexType := it.newIndexParams[common.IndexTypeKey]
	if indexType == indexparamcheck.IndexDISKANN {
		// check index node support disk index
		if !Params.IndexNodeCfg.EnableDisk.GetAsBool() {
			log.Warn("IndexNode don't support build disk index",
				zap.String("index type", it.newIndexParams[common.IndexTypeKey]),
				zap.Bool("enable disk", Params.IndexNodeCfg.EnableDisk.GetAsBool()))
			return errors.New("index node don't support build disk index")
		}

		// check load size and size of field data
		localUsedSize, err := indexcgowrapper.GetLocalUsedSize(paramtable.Get().LocalStorageCfg.Path.GetValue())
		if err != nil {
			log.Warn("IndexNode get local used size failed")
			return err
		}
		fieldDataSize, err := estimateFieldDataSize(it.req.GetDim(), it.req.GetNumRows(), it.req.GetField().GetDataType())
		if err != nil {
			log.Warn("IndexNode get local used size failed")
			return err
		}
		usedLocalSizeWhenBuild := int64(float64(fieldDataSize)*diskUsageRatio) + localUsedSize
		maxUsedLocalSize := int64(Params.IndexNodeCfg.DiskCapacityLimit.GetAsFloat() * Params.IndexNodeCfg.MaxDiskUsagePercentage.GetAsFloat())

		if usedLocalSizeWhenBuild > maxUsedLocalSize {
			log.Warn("IndexNode don't has enough disk size to build disk ann index",
				zap.Int64("usedLocalSizeWhenBuild", usedLocalSizeWhenBuild),
				zap.Int64("maxUsedLocalSize", maxUsedLocalSize))
			return errors.New("index node don't has enough disk size to build disk ann index")
		}

		err = indexparams.SetDiskIndexBuildParams(it.newIndexParams, int64(fieldDataSize))
		if err != nil {
			log.Warn("failed to fill disk index params", zap.Error(err))
			return err
		}
	}

	storageConfig := &indexcgopb.StorageConfig{
		Address:          it.req.GetStorageConfig().GetAddress(),
		AccessKeyID:      it.req.GetStorageConfig().GetAccessKeyID(),
		SecretAccessKey:  it.req.GetStorageConfig().GetSecretAccessKey(),
		UseSSL:           it.req.GetStorageConfig().GetUseSSL(),
		BucketName:       it.req.GetStorageConfig().GetBucketName(),
		RootPath:         it.req.GetStorageConfig().GetRootPath(),
		UseIAM:           it.req.GetStorageConfig().GetUseIAM(),
		IAMEndpoint:      it.req.GetStorageConfig().GetIAMEndpoint(),
		StorageType:      it.req.GetStorageConfig().GetStorageType(),
		UseVirtualHost:   it.req.GetStorageConfig().GetUseVirtualHost(),
		Region:           it.req.GetStorageConfig().GetRegion(),
		CloudProvider:    it.req.GetStorageConfig().GetCloudProvider(),
		RequestTimeoutMs: it.req.GetStorageConfig().GetRequestTimeoutMs(),
		SslCACert:        it.req.GetStorageConfig().GetSslCACert(),
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
		ClusterID:             it.req.GetClusterID(),
		BuildID:               it.req.GetBuildID(),
		CollectionID:          it.req.GetCollectionID(),
		PartitionID:           it.req.GetPartitionID(),
		SegmentID:             it.req.GetSegmentID(),
		IndexVersion:          it.req.GetIndexVersion(),
		CurrentIndexVersion:   it.req.GetCurrentIndexVersion(),
		NumRows:               it.req.GetNumRows(),
		Dim:                   it.req.GetDim(),
		IndexFilePrefix:       it.req.GetIndexFilePrefix(),
		InsertFiles:           it.req.GetDataPaths(),
		FieldSchema:           it.req.GetField(),
		StorageConfig:         storageConfig,
		IndexParams:           mapToKVPairs(it.newIndexParams),
		TypeParams:            mapToKVPairs(it.newTypeParams),
		StorePath:             it.req.GetStorePath(),
		StoreVersion:          it.req.GetStoreVersion(),
		IndexStorePath:        it.req.GetIndexStorePath(),
		OptFields:             optFields,
		PartitionKeyIsolation: it.req.GetPartitionKeyIsolation(),
	}

	log.Info("debug create index", zap.Any("buildIndexParams", buildIndexParams))
	var err error
	it.index, err = indexcgowrapper.CreateIndex(ctx, buildIndexParams)
	if err != nil {
		if it.index != nil && it.index.CleanLocalData() != nil {
			log.Warn("failed to clean cached data on disk after build index failed")
		}
		log.Warn("failed to build index", zap.Error(err))
		return err
	}

	buildIndexLatency := it.tr.RecordSpan()
	metrics.IndexNodeKnowhereBuildIndexLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(buildIndexLatency.Seconds())

	log.Info("Successfully build index")
	return nil
}

func (it *indexBuildTask) PostExecute(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("clusterID", it.req.GetClusterID()), zap.Int64("buildID", it.req.GetBuildID()),
		zap.Int64("collection", it.req.GetCollectionID()), zap.Int64("segmentID", it.req.GetSegmentID()),
		zap.Int32("currentIndexVersion", it.req.GetCurrentIndexVersion()))

	gcIndex := func() {
		if err := it.index.Delete(); err != nil {
			log.Warn("IndexNode indexBuildTask Execute CIndexDelete failed", zap.Error(err))
		}
	}
	indexFilePath2Size, err := it.index.UpLoad()
	if err != nil {
		log.Warn("failed to upload index", zap.Error(err))
		gcIndex()
		return err
	}
	encodeIndexFileDur := it.tr.Record("index serialize and upload done")
	metrics.IndexNodeEncodeIndexFileLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(encodeIndexFileDur.Seconds())

	// early release index for gc, and we can ensure that Delete is idempotent.
	gcIndex()

	// use serialized size before encoding
	var serializedSize uint64
	saveFileKeys := make([]string, 0)
	for filePath, fileSize := range indexFilePath2Size {
		serializedSize += uint64(fileSize)
		parts := strings.Split(filePath, "/")
		fileKey := parts[len(parts)-1]
		saveFileKeys = append(saveFileKeys, fileKey)
	}

	it.node.storeIndexFilesAndStatistic(it.req.GetClusterID(), it.req.GetBuildID(), saveFileKeys, serializedSize, it.req.GetCurrentIndexVersion())
	log.Debug("save index files done", zap.Strings("IndexFiles", saveFileKeys))
	saveIndexFileDur := it.tr.RecordSpan()
	metrics.IndexNodeSaveIndexFileLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(saveIndexFileDur.Seconds())
	it.tr.Elapse("index building all done")
	log.Info("Successfully save index files")
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
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return err
		}
		return err
	}

	var insertCodec storage.InsertCodec
	collectionID, partitionID, segmentID, insertData, err := insertCodec.DeserializeAll([]*Blob{{Key: toLoadDataPaths[0], Value: data}})
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
