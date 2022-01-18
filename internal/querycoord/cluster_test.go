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

package querycoord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	indexID           = UniqueID(0)
	dimKey            = "dim"
	rowIDFieldID      = 0
	timestampFieldID  = 1
	indexName         = "query-coord-index-0"
	defaultDim        = 128
	defaultMetricType = "JACCARD"
	defaultKVRootPath = "query-coord-unittest"
)

type constFieldParam struct {
	id       int64
	dataType schemapb.DataType
}

var simpleConstField = constFieldParam{
	id:       101,
	dataType: schemapb.DataType_Int32,
}

type vecFieldParam struct {
	id         int64
	dim        int
	metricType string
	vecType    schemapb.DataType
}

var simpleVecField = vecFieldParam{
	id:         100,
	dim:        defaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_FloatVector,
}

var timestampField = constFieldParam{
	id:       timestampFieldID,
	dataType: schemapb.DataType_Int64,
}

var uidField = constFieldParam{
	id:       rowIDFieldID,
	dataType: schemapb.DataType_Int64,
}

type indexParam = map[string]string

func segSizeEstimateForTest(segments *querypb.LoadSegmentsRequest, dataKV kv.DataKV) (int64, error) {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(segments.Schema)
	if err != nil {
		return 0, err
	}
	sizeOfReq := int64(0)
	for _, loadInfo := range segments.Infos {
		sizeOfReq += int64(sizePerRecord) * loadInfo.NumOfRows
	}

	return sizeOfReq, nil
}

func genCollectionMeta(collectionID UniqueID, schema *schemapb.CollectionSchema) *etcdpb.CollectionMeta {
	colInfo := &etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       schema,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}
	return colInfo
}

// ---------- unittest util functions ----------
// functions of inserting data init
func genInsertData(msgLength int, schema *schemapb.CollectionSchema) (*storage.InsertData, error) {
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			data := make([]bool, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = true
			}
			insertData.Data[f.FieldID] = &storage.BoolFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int8:
			data := make([]int8, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int8(i)
			}
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int16:
			data := make([]int16, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int16(i)
			}
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int32:
			data := make([]int32, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int32(i)
			}
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int64:
			data := make([]int64, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int64(i)
			}
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Float:
			data := make([]float32, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = float32(i)
			}
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Double:
			data := make([]float64, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = float64(i)
			}
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_FloatVector:
			dim := simpleVecField.dim // if no dim specified, use simpleVecField's dim
			for _, p := range f.TypeParams {
				if p.Key == dimKey {
					var err error
					dim, err = strconv.Atoi(p.Value)
					if err != nil {
						return nil, err
					}
				}
			}
			data := make([]float32, 0)
			for i := 0; i < msgLength; i++ {
				for j := 0; j < dim; j++ {
					data = append(data, float32(i*j)*0.1)
				}
			}
			insertData.Data[f.FieldID] = &storage.FloatVectorFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
				Dim:     dim,
			}
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return insertData, nil
}

func genStorageBlob(collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	msgLength int,
	schema *schemapb.CollectionSchema) ([]*storage.Blob, error) {
	collMeta := genCollectionMeta(collectionID, schema)
	inCodec := storage.NewInsertCodec(collMeta)
	insertData, err := genInsertData(msgLength, schema)
	if err != nil {
		return nil, err
	}
	// timestamp field not allowed 0 timestamp
	if _, ok := insertData.Data[timestampFieldID]; ok {
		insertData.Data[timestampFieldID].(*storage.Int64FieldData).Data[0] = 1
	}
	binLogs, _, err := inCodec.Serialize(partitionID, segmentID, insertData)

	return binLogs, err
}

func saveSimpleBinLog(ctx context.Context, schema *schemapb.CollectionSchema, dataKV kv.DataKV) ([]*datapb.FieldBinlog, error) {
	return saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultNumRowPerSegment, schema, dataKV)
}

func saveBinLog(ctx context.Context,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	msgLength int,
	schema *schemapb.CollectionSchema,
	dataKV kv.DataKV) ([]*datapb.FieldBinlog, error) {
	binLogs, err := genStorageBlob(collectionID, partitionID, segmentID, msgLength, schema)
	if err != nil {
		return nil, err
	}

	log.Debug(".. [query coord unittest] Saving bin logs to MinIO ..", zap.Int("number", len(binLogs)))
	kvs := make(map[string]string, len(binLogs))

	// write insert binlog
	fieldBinlog := make([]*datapb.FieldBinlog, 0)
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		log.Debug("[QueryCoord unittest] save binlog", zap.Int64("fieldID", fieldID))
		if err != nil {
			return nil, err
		}

		key := genKey(collectionID, partitionID, segmentID, fieldID)
		kvs[key] = string(blob.Value[:])
		fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{LogPath: key}},
		})
	}
	log.Debug("[QueryCoord unittest] save binlog file to MinIO/S3")

	err = dataKV.MultiSave(kvs)
	return fieldBinlog, err
}

func genKey(collectionID, partitionID, segmentID UniqueID, fieldID int64) string {
	ids := []string{
		defaultKVRootPath,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10),
		strconv.FormatInt(fieldID, 10),
	}
	return path.Join(ids...)
}

func genSimpleIndexParams() indexParam {
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = strconv.FormatInt(defaultDim, 10)
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "400"
	return indexParams
}

func generateIndex(segmentID UniqueID) ([]string, error) {
	indexParams := genSimpleIndexParams()

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(defaultDim)
	var indexRowData []float32
	for n := 0; n < defaultNumRowPerSegment; n++ {
		for i := 0; i < defaultDim; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}

	index, err := indexnode.NewCIndex(typeParams, indexParams)
	if err != nil {
		return nil, err
	}

	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	if err != nil {
		return nil, err
	}

	option := &minioKV.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, err
	}

	// save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, err
	}

	// serialize index params
	indexCodec := storage.NewIndexFileBinlogCodec()
	serializedIndexBlobs, err := indexCodec.Serialize(
		0,
		0,
		0,
		0,
		0,
		0,
		indexParams,
		indexName,
		indexID,
		binarySet,
	)
	if err != nil {
		return nil, err
	}

	indexPaths := make([]string, 0)
	for _, index := range serializedIndexBlobs {
		p := strconv.Itoa(int(segmentID)) + "/" + index.Key
		indexPaths = append(indexPaths, p)
		err := kv.Save(p, string(index.Value))
		if err != nil {
			return nil, err
		}
	}

	return indexPaths, nil
}

func TestQueryNodeCluster_getMetrics(t *testing.T) {
	log.Info("TestQueryNodeCluster_getMetrics, todo")
}

func TestReloadClusterFromKV(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(&Params.BaseParams)
	defer etcdCli.Close()
	assert.Nil(t, err)
	t.Run("Test LoadOnlineNodes", func(t *testing.T) {
		refreshParams()
		baseCtx := context.Background()
		kv := etcdkv.NewEtcdKV(etcdCli, Params.BaseParams.MetaRootPath)
		clusterSession := sessionutil.NewSession(context.Background(), Params.BaseParams.MetaRootPath, etcdCli)
		clusterSession.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, false)
		clusterSession.Register()
		cluster := &queryNodeCluster{
			ctx:              baseCtx,
			client:           kv,
			nodes:            make(map[int64]Node),
			newNodeFn:        newQueryNodeTest,
			session:          clusterSession,
			segSizeEstimator: segSizeEstimateForTest,
		}

		queryNode, err := startQueryNodeServer(baseCtx)
		assert.Nil(t, err)

		cluster.reloadFromKV()

		nodeID := queryNode.queryNodeID
		waitQueryNodeOnline(cluster, nodeID)

		queryNode.stop()
		err = removeNodeSession(queryNode.queryNodeID)
		assert.Nil(t, err)
	})

	t.Run("Test LoadOfflineNodes", func(t *testing.T) {
		refreshParams()
		ctx, cancel := context.WithCancel(context.Background())
		kv := etcdkv.NewEtcdKV(etcdCli, Params.BaseParams.MetaRootPath)
		clusterSession := sessionutil.NewSession(context.Background(), Params.BaseParams.MetaRootPath, etcdCli)
		clusterSession.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, false)
		clusterSession.Register()
		factory := msgstream.NewPmsFactory()
		handler, err := newChannelUnsubscribeHandler(ctx, kv, factory)
		assert.Nil(t, err)
		meta, err := newMeta(ctx, kv, factory, nil)
		assert.Nil(t, err)

		cluster := &queryNodeCluster{
			client:           kv,
			handler:          handler,
			clusterMeta:      meta,
			nodes:            make(map[int64]Node),
			newNodeFn:        newQueryNodeTest,
			session:          clusterSession,
			segSizeEstimator: segSizeEstimateForTest,
		}

		kvs := make(map[string]string)
		session := &sessionutil.Session{
			ServerID: 100,
			Address:  "localhost",
		}
		sessionBlob, err := json.Marshal(session)
		assert.Nil(t, err)
		sessionKey := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, 100)
		kvs[sessionKey] = string(sessionBlob)

		err = kv.MultiSave(kvs)
		assert.Nil(t, err)

		cluster.reloadFromKV()
		assert.Equal(t, 1, len(cluster.nodes))

		err = removeAllSession()
		assert.Nil(t, err)
		cancel()
	})
}

func TestGrpcRequest(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.BaseParams)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.BaseParams.MetaRootPath)
	clusterSession := sessionutil.NewSession(context.Background(), Params.BaseParams.MetaRootPath, etcdCli)
	clusterSession.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, false)
	clusterSession.Register()
	factory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarCfg.Address,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = factory.SetParams(m)
	assert.Nil(t, err)
	idAllocator := func() (UniqueID, error) {
		return 0, nil
	}
	meta, err := newMeta(baseCtx, kv, factory, idAllocator)
	assert.Nil(t, err)

	handler, err := newChannelUnsubscribeHandler(baseCtx, kv, factory)
	assert.Nil(t, err)

	cluster := &queryNodeCluster{
		ctx:              baseCtx,
		cancel:           cancel,
		client:           kv,
		clusterMeta:      meta,
		handler:          handler,
		nodes:            make(map[int64]Node),
		newNodeFn:        newQueryNodeTest,
		session:          clusterSession,
		segSizeEstimator: segSizeEstimateForTest,
	}

	t.Run("Test GetNodeInfoByIDWithNodeNotExist", func(t *testing.T) {
		_, err := cluster.getNodeInfoByID(defaultQueryNodeID)
		assert.NotNil(t, err)
	})

	t.Run("Test GetSegmentInfoByNodeWithNodeNotExist", func(t *testing.T) {
		getSegmentInfoReq := &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: defaultCollectionID,
		}
		_, err = cluster.getSegmentInfoByNode(baseCtx, defaultQueryNodeID, getSegmentInfoReq)
		assert.NotNil(t, err)
	})

	node, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	nodeSession := node.session
	nodeID := node.queryNodeID
	cluster.registerNode(baseCtx, nodeSession, nodeID, disConnect)
	waitQueryNodeOnline(cluster, nodeID)

	t.Run("Test GetComponentInfos", func(t *testing.T) {
		infos := cluster.getComponentInfos(baseCtx)
		assert.Equal(t, 1, len(infos))
	})

	t.Run("Test LoadSegments", func(t *testing.T) {
		segmentLoadInfo := &querypb.SegmentLoadInfo{
			SegmentID:    defaultSegmentID,
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
		}
		loadSegmentReq := &querypb.LoadSegmentsRequest{
			DstNodeID:    nodeID,
			Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
			Schema:       genCollectionSchema(defaultCollectionID, false),
			CollectionID: defaultCollectionID,
		}
		err := cluster.loadSegments(baseCtx, nodeID, loadSegmentReq)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseSegments", func(t *testing.T) {
		releaseSegmentReq := &querypb.ReleaseSegmentsRequest{
			NodeID:       nodeID,
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}
		err := cluster.releaseSegments(baseCtx, nodeID, releaseSegmentReq)
		assert.Nil(t, err)
	})

	t.Run("Test AddQueryChannel", func(t *testing.T) {
		info := cluster.clusterMeta.getQueryChannelInfoByID(defaultCollectionID)
		addQueryChannelReq := &querypb.AddQueryChannelRequest{
			NodeID:             nodeID,
			CollectionID:       defaultCollectionID,
			QueryChannel:       info.QueryChannel,
			QueryResultChannel: info.QueryResultChannel,
		}
		err = cluster.addQueryChannel(baseCtx, nodeID, addQueryChannelReq)
		assert.Nil(t, err)
	})

	t.Run("Test RemoveQueryChannel", func(t *testing.T) {
		info := cluster.clusterMeta.getQueryChannelInfoByID(defaultCollectionID)
		removeQueryChannelReq := &querypb.RemoveQueryChannelRequest{
			NodeID:             nodeID,
			CollectionID:       defaultCollectionID,
			QueryChannel:       info.QueryChannel,
			QueryResultChannel: info.QueryResultChannel,
		}
		err = cluster.removeQueryChannel(baseCtx, nodeID, removeQueryChannelReq)
		assert.Nil(t, err)
	})

	t.Run("Test GetSegmentInfo", func(t *testing.T) {
		getSegmentInfoReq := &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: defaultCollectionID,
		}
		_, err = cluster.getSegmentInfo(baseCtx, getSegmentInfoReq)
		assert.Nil(t, err)
	})

	t.Run("Test GetSegmentInfoByNode", func(t *testing.T) {
		getSegmentInfoReq := &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: defaultCollectionID,
		}
		_, err = cluster.getSegmentInfoByNode(baseCtx, nodeID, getSegmentInfoReq)
		assert.Nil(t, err)
	})

	node.getSegmentInfos = returnFailedGetSegmentInfoResult

	t.Run("Test GetSegmentInfoFailed", func(t *testing.T) {
		getSegmentInfoReq := &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: defaultCollectionID,
		}
		_, err = cluster.getSegmentInfo(baseCtx, getSegmentInfoReq)
		assert.NotNil(t, err)
	})

	t.Run("Test GetSegmentInfoByNodeFailed", func(t *testing.T) {
		getSegmentInfoReq := &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: defaultCollectionID,
		}
		_, err = cluster.getSegmentInfoByNode(baseCtx, nodeID, getSegmentInfoReq)
		assert.NotNil(t, err)
	})

	node.getSegmentInfos = returnSuccessGetSegmentInfoResult

	t.Run("Test GetNodeInfoByID", func(t *testing.T) {
		res, err := cluster.getNodeInfoByID(nodeID)
		assert.Nil(t, err)
		assert.NotNil(t, res)
	})

	node.getMetrics = returnFailedGetMetricsResult

	t.Run("Test GetNodeInfoByIDFailed", func(t *testing.T) {
		_, err := cluster.getNodeInfoByID(nodeID)
		assert.NotNil(t, err)
	})

	node.getMetrics = returnSuccessGetMetricsResult

	cluster.stopNode(nodeID)
	t.Run("Test GetSegmentInfoByNodeAfterNodeStop", func(t *testing.T) {
		getSegmentInfoReq := &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			CollectionID: defaultCollectionID,
		}
		_, err = cluster.getSegmentInfoByNode(baseCtx, nodeID, getSegmentInfoReq)
		assert.NotNil(t, err)
	})

	err = removeAllSession()
	assert.Nil(t, err)
}

func TestEstimateSegmentSize(t *testing.T) {
	refreshParams()
	binlog := []*datapb.FieldBinlog{
		{
			FieldID: defaultVecFieldID,
			Binlogs: []*datapb.Binlog{{LogPath: "by-dev/rand/path", LogSize: 1024}},
		},
	}

	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    defaultSegmentID,
		PartitionID:  defaultPartitionID,
		CollectionID: defaultCollectionID,
		BinlogPaths:  binlog,
		NumOfRows:    defaultNumRowPerSegment,
	}

	loadReq := &querypb.LoadSegmentsRequest{
		Infos:        []*querypb.SegmentLoadInfo{loadInfo},
		CollectionID: defaultCollectionID,
	}

	size, err := estimateSegmentsSize(loadReq, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1024), size)

	indexInfo := &querypb.VecFieldIndexInfo{
		FieldID:     defaultVecFieldID,
		EnableIndex: true,
		IndexSize:   2048,
	}

	loadInfo.IndexInfos = []*querypb.VecFieldIndexInfo{indexInfo}
	size, err = estimateSegmentsSize(loadReq, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2048), size)
}

func TestSetNodeState(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.BaseParams)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.BaseParams.MetaRootPath)
	clusterSession := sessionutil.NewSession(context.Background(), Params.BaseParams.MetaRootPath, etcdCli)
	clusterSession.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, false)
	clusterSession.Register()
	factory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarCfg.Address,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = factory.SetParams(m)
	assert.Nil(t, err)
	idAllocator := func() (UniqueID, error) {
		return 0, nil
	}
	meta, err := newMeta(baseCtx, kv, factory, idAllocator)
	assert.Nil(t, err)

	handler, err := newChannelUnsubscribeHandler(baseCtx, kv, factory)
	assert.Nil(t, err)

	cluster := &queryNodeCluster{
		ctx:              baseCtx,
		cancel:           cancel,
		client:           kv,
		clusterMeta:      meta,
		handler:          handler,
		nodes:            make(map[int64]Node),
		newNodeFn:        newQueryNodeTest,
		session:          clusterSession,
		segSizeEstimator: segSizeEstimateForTest,
	}

	node, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	err = cluster.registerNode(baseCtx, node.session, node.queryNodeID, disConnect)
	assert.Nil(t, err)
	waitQueryNodeOnline(cluster, node.queryNodeID)

	dmChannelWatchInfo := &querypb.DmChannelWatchInfo{
		CollectionID: defaultCollectionID,
		DmChannel:    "test-dmChannel",
		NodeIDLoaded: node.queryNodeID,
	}
	err = meta.setDmChannelInfos([]*querypb.DmChannelWatchInfo{dmChannelWatchInfo})
	assert.Nil(t, err)
	deltaChannelInfo := &datapb.VchannelInfo{
		CollectionID: defaultCollectionID,
		ChannelName:  "test-deltaChannel",
	}
	err = meta.setDeltaChannel(defaultCollectionID, []*datapb.VchannelInfo{deltaChannelInfo})
	assert.Nil(t, err)

	nodeInfo, err := cluster.getNodeInfoByID(node.queryNodeID)
	assert.Nil(t, err)
	cluster.setNodeState(node.queryNodeID, nodeInfo, offline)
	assert.Equal(t, 1, len(handler.downNodeChan))

	node.stop()
	removeAllSession()
	cancel()
}
