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
	"errors"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexnodepb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

const (
	// paramsKeyToParse is the key of the param to build index.
	paramsKeyToParse = "params"
)

var (
	cancelErr = fmt.Errorf("cancelled")
)

type Blob = storage.Blob

type taskInfo struct {
	cancel     context.CancelFunc
	state      commonpb.IndexState
	indexfiles []string

	// task statistics
	statistic *indexnodepb.JobInfo
}

type task interface {
	Ctx() context.Context
	Name() string
	Prepare(context.Context) error
	LoadData(context.Context) error
	BuildIndex(context.Context) error
	SaveIndexFiles(context.Context) error
	OnEnqueue(context.Context) error
	SetState(state commonpb.IndexState)
	GetState() commonpb.IndexState
	Reset()
}

// IndexBuildTask is used to record the information of the index tasks.
type indexBuildTask struct {
	ident  string
	cancel context.CancelFunc
	ctx    context.Context

	cm             storage.ChunkManager
	index          indexcgowrapper.CodecIndex
	savePaths      []string
	req            *indexpb.CreateJobRequest
	BuildID        UniqueID
	nodeID         UniqueID
	ClusterID      UniqueID
	collectionID   UniqueID
	partitionID    UniqueID
	segmentID      UniqueID
	fieldID        UniqueID
	fieldData      storage.FieldData
	indexBlobs     []*storage.Blob
	newTypeParams  map[string]string
	newIndexParams map[string]string
	serializedSize uint64
	tr             *timerecord.TimeRecorder
	statistic      indexnodepb.JobInfo
	node           *IndexNode
}

func (it *indexBuildTask) Reset() {
	it.ident = ""
	it.cancel = nil
	it.ctx = nil
	it.cm = nil
	it.index = nil
	it.savePaths = nil
	it.req = nil
	it.fieldData = nil
	it.indexBlobs = nil
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

func (it *indexBuildTask) SetState(state commonpb.IndexState) {
	it.node.storeTaskState(it.ClusterID, it.BuildID, state)
}

func (it *indexBuildTask) GetState() commonpb.IndexState {
	state, ok := it.node.loadTaskState(it.ClusterID, it.BuildID)
	if !ok {
		return commonpb.IndexState_IndexStateNone
	}
	return state
}

// OnEnqueue enqueues indexing tasks.
func (it *indexBuildTask) OnEnqueue(ctx context.Context) error {
	it.statistic.StartTime = time.Now().UnixMicro()
	it.statistic.PodID = it.node.GetNodeID()
	logutil.Logger(ctx).Debug("IndexNode IndexBuilderTask Enqueue")
	return nil
}

func (it *indexBuildTask) Prepare(ctx context.Context) error {
	typeParams := make(map[string]string)
	for _, kvPair := range it.req.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		_, ok := typeParams[key]
		if ok {
			return errors.New("duplicated key in type params")
		}
		if key == paramsKeyToParse {
			params, err := funcutil.ParseIndexParamsMap(value)
			if err != nil {
				return err
			}
			for pk, pv := range params {
				typeParams[pk] = pv
			}
		} else {
			typeParams[key] = value
		}
	}

	indexParams := make(map[string]string)
	for _, kvPair := range it.req.GetIndexParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		_, ok := indexParams[key]
		if ok {
			return errors.New("duplicated key in index params")
		}
		if key == paramsKeyToParse {
			params, err := funcutil.ParseIndexParamsMap(value)
			if err != nil {
				return err
			}
			for pk, pv := range params {
				indexParams[pk] = pv
			}
		} else {
			indexParams[key] = value
		}
	}
	it.newTypeParams = typeParams
	it.newIndexParams = indexParams
	it.statistic.IndexParams = it.req.GetIndexParams()
	// ugly codes to get dimension
	if dimStr, ok := typeParams["dim"]; ok {
		var err error
		it.statistic.Dim, err = strconv.ParseInt(dimStr, 10, 64)
		if err != nil {
			logutil.Logger(ctx).Error("parse dimesion failed", zap.Error(err))
			// ignore error
		}
	}
	// setup chunkmanager
	// opts := make([]storage.Option, 0)
	// // TODO: secret access key_id
	// opts = append(opts, storage.AccessKeyID(it.req.StorageAccessKey))
	// opts = append(opts, storage.BucketName(it.req.BucketName))
	// factory := storage.NewChunkManagerFactory("local", "minio", opts...)
	// var err error
	// it.cm, err = factory.NewVectorStorageChunkManager(ctx)
	// if err != nil {
	// 	logutil.Logger(ctx).Error("init chunk manager failed", zap.Error(err), zap.String("BucketName", it.req.BucketName), zap.String("StorageAccessKey", it.req.StorageAccessKey))
	// 	return err
	// }
	return nil
}

func (it *indexBuildTask) LoadData(ctx context.Context) error {
	getValueByPath := func(path string) ([]byte, error) {
		data, err := it.cm.Read(path)
		if err != nil {
			if errors.Is(err, ErrNoSuchKey) {
				return nil, ErrNoSuchKey
			}
			return nil, err
		}
		return data, nil
	}
	getBlobByPath := func(path string) (*Blob, error) {
		value, err := getValueByPath(path)
		if err != nil {
			return nil, err
		}
		return &Blob{
			Key:   path,
			Value: value,
		}, nil
	}

	toLoadDataPaths := it.req.GetDataPaths()
	keys := make([]string, len(toLoadDataPaths))
	blobs := make([]*Blob, len(toLoadDataPaths))

	loadKey := func(idx int) error {
		keys[idx] = toLoadDataPaths[idx]
		blob, err := getBlobByPath(toLoadDataPaths[idx])
		if err != nil {
			return err
		}
		blobs[idx] = blob
		return nil
	}
	// Use runtime.GOMAXPROCS(0) instead of runtime.NumCPU()
	// to respect CPU quota of container/pod
	// gomaxproc will be set by `automaxproc`, passing 0 will just retrieve the value
	err := funcutil.ProcessFuncParallel(len(toLoadDataPaths), runtime.GOMAXPROCS(0), loadKey, "loadKey")
	if err != nil {
		logutil.Logger(it.ctx).Warn("loadKey failed", zap.Error(err))
		return err
	}
	loadVectorDuration := it.tr.RecordSpan().Milliseconds()
	logutil.Logger(ctx).Debug("indexnode load data success")
	it.tr.Record("load field data done")
	metrics.IndexNodeLoadFieldLatency.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10)).Observe(float64(loadVectorDuration))

	return it.decodeBlobs(ctx, blobs)
}

func (it *indexBuildTask) BuildIndex(ctx context.Context) error {
	dataset := indexcgowrapper.GenDataset(it.fieldData)
	dType := dataset.DType
	var err error
	if dType != schemapb.DataType_None {
		it.index, err = indexcgowrapper.NewCgoIndex(dType, it.newTypeParams, it.newIndexParams)
		if err != nil {
			logutil.Logger(ctx).Error("failed to create index", zap.Error(err))
			return err
		}

		err = it.index.Build(dataset)
		if err != nil {
			logutil.Logger(ctx).Error("failed to build index", zap.Error(err))
			return err
		}
	}
	metrics.IndexNodeKnowhereBuildIndexLatency.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10)).Observe(float64(it.tr.RecordSpan().Milliseconds()))

	it.tr.Record("build index done")
	indexBlobs, err := it.index.Serialize()
	if err != nil {
		logutil.Logger(ctx).Error("IndexNode index Serialize failed", zap.Error(err))
		return err
	}
	it.tr.Record("index serialize done")

	// use serialized size before encoding
	it.serializedSize = 0
	for _, blob := range indexBlobs {
		it.serializedSize += uint64(len(blob.Value))
	}

	// early release index for gc, and we can ensure that Delete is idempotent.
	if err := it.index.Delete(); err != nil {
		logutil.Logger(it.ctx).Error("IndexNode indexBuildTask Execute CIndexDelete failed", zap.Error(err))
	}

	var serializedIndexBlobs []*storage.Blob
	codec := storage.NewIndexFileBinlogCodec()
	serializedIndexBlobs, err = codec.Serialize(
		it.req.BuildID,
		it.req.IndexVersion,
		it.collectionID,
		it.partitionID,
		it.segmentID,
		it.fieldID,
		it.newIndexParams,
		it.req.IndexName,
		it.req.IndexID,
		indexBlobs,
	)
	if err != nil {
		return err
	}
	encodeIndexFileDur := it.tr.Record("index codec serialize done")
	metrics.IndexNodeEncodeIndexFileLatency.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10)).Observe(float64(encodeIndexFileDur.Milliseconds()))
	it.indexBlobs = serializedIndexBlobs
	return nil
}

func (it *indexBuildTask) SaveIndexFiles(ctx context.Context) error {
	blobCnt := len(it.indexBlobs)
	getSavePathByKey := func(key string) string {
		return path.Join(it.req.IndexFilePrefix, strconv.Itoa(int(it.req.BuildID)), strconv.Itoa(int(it.req.IndexVersion)),
			strconv.Itoa(int(it.partitionID)), strconv.Itoa(int(it.segmentID)), key)
	}

	savePaths := make([]string, blobCnt)
	saveIndexFile := func(idx int) error {
		blob := it.indexBlobs[idx]
		savePath := getSavePathByKey(blob.Key)
		saveFn := func() error {
			return it.cm.Write(savePath, blob.Value)
		}
		if err := retry.Do(ctx, saveFn, retry.Attempts(5)); err != nil {
			logutil.Logger(ctx).Warn("index node save index file failed", zap.Error(err), zap.String("savePath", savePath))
			return err
		}
		savePaths[idx] = savePath
		return nil
	}

	// If an error occurs, return the error that the task state will be set to retry.
	if err := funcutil.ProcessFuncParallel(blobCnt, runtime.NumCPU(), saveIndexFile, "saveIndexFile"); err != nil {
		logutil.Logger(it.ctx).Error("saveIndexFile fail")
		return err
	}
	it.savePaths = savePaths
	it.statistic.EndTime = time.Now().UnixMicro()
	it.node.storeIndexFilesAndStatistic(it.ClusterID, it.BuildID, savePaths, &it.statistic)
	logutil.Logger(ctx).Debug("save index files done", zap.Strings("IndexFiles", savePaths))
	saveIndexFileDur := it.tr.Record("index file save done")
	metrics.IndexNodeSaveIndexFileLatency.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10)).Observe(float64(saveIndexFileDur.Milliseconds()))
	it.tr.Elapse("index building all done")
	logutil.Logger(ctx).Info("IndexNode CreateIndex successfully ", zap.Int64("collect", it.collectionID),
		zap.Int64("partition", it.partitionID), zap.Int64("segment", it.segmentID))
	return nil
}

func (it *indexBuildTask) decodeBlobs(ctx context.Context, blobs []*storage.Blob) error {
	var insertCodec storage.InsertCodec
	collectionID, partitionID, segmentID, insertData, err2 := insertCodec.DeserializeAll(blobs)
	if err2 != nil {
		return err2
	}
	decodeDuration := it.tr.RecordSpan().Milliseconds()
	metrics.IndexNodeDecodeFieldLatency.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10)).Observe(float64(decodeDuration))

	if len(insertData.Data) != 1 {
		return errors.New("we expect only one field in deserialized insert data")
	}
	it.collectionID = collectionID
	it.partitionID = partitionID
	it.segmentID = segmentID

	logutil.Logger(ctx).Debug("indexnode deserialize data success",
		zap.Int64("index id", it.req.IndexID),
		zap.String("index name", it.req.IndexName),
		zap.Int64("collectionID", it.collectionID),
		zap.Int64("partitionID", it.partitionID),
		zap.Int64("segmentID", it.segmentID))

	it.tr.Record("deserialize vector data done")

	// we can ensure that there blobs are in one Field
	var data storage.FieldData
	var fieldID storage.FieldID
	for fID, value := range insertData.Data {
		data = value
		fieldID = fID
		break
	}
	it.statistic.NumRows = int64(data.RowNum())
	it.fieldID = fieldID
	it.fieldData = data
	return nil
}

// Execute actually performs the task of building an index.
// func (it *indexBuildTask) Execute(ctx context.Context) error {
// 	logutil.Logger(it.ctx).Debug("IndexNode indexBuildTask Execute ...")
// 	sp, _ := trace.StartSpanFromContextWithOperationName(ctx, "CreateIndex-Execute")
// 	defer sp.Finish()
// 	select {
// 	case <-ctx.Done():
// 		logutil.Logger(it.ctx).Warn("build task was cancelled")
// 		return cancelErr
// 	default:
// 		if err := it.prepareParams(ctx); err != nil {
// 			it.SetState(commonpb.IndexState_Failed)
// 			logutil.Logger(it.ctx).Error("IndexNode indexBuildTask Execute prepareParams failed", zap.Error(err))
// 			return err
// 		}
// 		defer it.releaseMemory()
// 		blobs, err := it.buildIndex(ctx)
// 		if err != nil {
// 			if errors.Is(err, ErrNoSuchKey) {
// 				it.SetState(commonpb.IndexState_Failed)
// 				logutil.Logger(it.ctx).Error("IndexNode indexBuildTask Execute buildIndex failed", zap.Error(err))
// 				return err
// 			}
// 			it.SetState(commonpb.IndexState_Unissued)
// 			logutil.Logger(it.ctx).Error("IndexNode indexBuildTask Execute buildIndex failed, need to retry", zap.Error(err))
// 			return err
// 		}
// 		if err = it.saveIndex(ctx, blobs); err != nil {
// 			logutil.Logger(it.ctx).Warn("save index file failed", zap.Error(err))
// 			it.SetState(commonpb.IndexState_Unissued)
// 			return err
// 		}
// 		it.SetState(commonpb.IndexState_Finished)
// 		saveIndexFileDur := it.tr.Record("index file save done")
// 		metrics.IndexNodeSaveIndexFileLatency.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10)).Observe(float64(saveIndexFileDur.Milliseconds()))
// 		it.tr.Elapse("index building all done")
// 		logutil.Logger(it.ctx).Info("IndexNode CreateIndex successfully ", zap.Int64("collect", it.collectionID),
// 			zap.Int64("partition", it.partitionID), zap.Int64("segment", it.segmentID))
// 		return nil
// 	}
// }
