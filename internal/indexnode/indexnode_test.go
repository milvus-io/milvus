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
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

//func TestRegister(t *testing.T) {
//	var (
//		factory = &mockFactory{}
//		ctx     = context.TODO()
//	)
//	Params.Init()
//	in, err := NewIndexNode(ctx, factory)
//	assert.NoError(t, err)
//	in.SetEtcdClient(getEtcdClient())
//	assert.Nil(t, in.initSession())
//	assert.Nil(t, in.Register())
//	key := in.session.ServerName
//	if !in.session.Exclusive {
//		key = fmt.Sprintf("%s-%d", key, in.session.ServerID)
//	}
//	resp, err := getEtcdClient().Get(ctx, path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot, key))
//	assert.NoError(t, err)
//	assert.Equal(t, int64(1), resp.Count)
//	sess := &sessionutil.Session{}
//	assert.Nil(t, json.Unmarshal(resp.Kvs[0].Value, sess))
//	assert.Equal(t, sess.ServerID, in.session.ServerID)
//	assert.Equal(t, sess.Address, in.session.Address)
//	assert.Equal(t, sess.ServerName, in.session.ServerName)
//
//	// revoke lease
//	in.session.Revoke(time.Second)
//
//	in.chunkManager = storage.NewLocalChunkManager(storage.RootPath("/tmp/lib/milvus"))
//	t.Run("CreateIndex FloatVector", func(t *testing.T) {
//		var insertCodec storage.InsertCodec
//
//		insertCodec.Schema = &etcdpb.CollectionMeta{
//			ID: collectionID,
//			Schema: &schemapb.CollectionSchema{
//				Fields: []*schemapb.FieldSchema{
//					{
//						FieldID:      floatVectorFieldID,
//						Name:         floatVectorFieldName,
//						IsPrimaryKey: false,
//						DataType:     schemapb.DataType_FloatVector,
//					},
//				},
//			},
//		}
//		data := make(map[UniqueID]storage.FieldData)
//		tsData := make([]int64, nb)
//		for i := 0; i < nb; i++ {
//			tsData[i] = int64(i + 100)
//		}
//		data[tsFieldID] = &storage.Int64FieldData{
//			NumRows: []int64{nb},
//			Data:    tsData,
//		}
//		data[floatVectorFieldID] = &storage.FloatVectorFieldData{
//			NumRows: []int64{nb},
//			Data:    generateFloatVectors(),
//			Dim:     dim,
//		}
//		insertData := storage.InsertData{
//			Data: data,
//			Infos: []storage.BlobInfo{
//				{
//					Length: 10,
//				},
//			},
//		}
//		binLogs, _, err := insertCodec.Serialize(999, 888, &insertData)
//		assert.NoError(t, err)
//		kvs := make(map[string][]byte, len(binLogs))
//		paths := make([]string, 0, len(binLogs))
//		for i, blob := range binLogs {
//			key := path.Join(floatVectorBinlogPath, strconv.Itoa(i))
//			paths = append(paths, key)
//			kvs[key] = blob.Value[:]
//		}
//		err = in.chunkManager.MultiWrite(kvs)
//		assert.NoError(t, err)
//
//		indexMeta := &indexpb.IndexMeta{
//			IndexBuildID: indexBuildID1,
//			State:        commonpb.IndexState_InProgress,
//			IndexVersion: 1,
//		}
//
//		value, err := proto.Marshal(indexMeta)
//		assert.NoError(t, err)
//		err = in.etcdKV.Save(metaPath1, string(value))
//		assert.NoError(t, err)
//		req := &indexpb.CreateIndexRequest{
//			IndexBuildID: indexBuildID1,
//			IndexName:    "FloatVector",
//			IndexID:      indexID,
//			Version:      1,
//			MetaPath:     metaPath1,
//			DataPaths:    paths,
//			TypeParams: []*commonpb.KeyValuePair{
//				{
//					Key:   common.DimKey,
//					Value: "8",
//				},
//			},
//			IndexParams: []*commonpb.KeyValuePair{
//				{
//					Key:   common.IndexTypeKey,
//					Value: "IVF_SQ8",
//				},
//				{
//					Key:   common.IndexParamsKey,
//					Value: "{\"nlist\": 128}",
//				},
//				{
//					Key:   common.MetricTypeKey,
//					Value: "L2",
//				},
//			},
//		}
//
//		status, err2 := in.CreateIndex(ctx, req)
//		assert.Nil(t, err2)
//		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
//
//		strValue, err3 := in.etcdKV.Load(metaPath1)
//		assert.Nil(t, err3)
//		indexMetaTmp := indexpb.IndexMeta{}
//		err = proto.Unmarshal([]byte(strValue), &indexMetaTmp)
//		assert.NoError(t, err)
//		for indexMetaTmp.State != commonpb.IndexState_Finished {
//			time.Sleep(100 * time.Millisecond)
//			strValue, err := in.etcdKV.Load(metaPath1)
//			assert.NoError(t, err)
//			err = proto.Unmarshal([]byte(strValue), &indexMetaTmp)
//			assert.NoError(t, err)
//		}
//		defer in.chunkManager.MultiRemove(indexMetaTmp.IndexFileKeys)
//		defer func() {
//			for k := range kvs {
//				err = in.chunkManager.Remove(k)
//				assert.NoError(t, err)
//			}
//		}()
//
//		defer in.etcdKV.RemoveWithPrefix(metaPath1)
//	})
//	t.Run("CreateIndex BinaryVector", func(t *testing.T) {
//		var insertCodec storage.InsertCodec
//
//		insertCodec.Schema = &etcdpb.CollectionMeta{
//			ID: collectionID,
//			Schema: &schemapb.CollectionSchema{
//				Fields: []*schemapb.FieldSchema{
//					{
//						FieldID:      binaryVectorFieldID,
//						Name:         binaryVectorFieldName,
//						IsPrimaryKey: false,
//						DataType:     schemapb.DataType_BinaryVector,
//					},
//				},
//			},
//		}
//		data := make(map[UniqueID]storage.FieldData)
//		tsData := make([]int64, nb)
//		for i := 0; i < nb; i++ {
//			tsData[i] = int64(i + 100)
//		}
//		data[tsFieldID] = &storage.Int64FieldData{
//			NumRows: []int64{nb},
//			Data:    tsData,
//		}
//		data[binaryVectorFieldID] = &storage.BinaryVectorFieldData{
//			NumRows: []int64{nb},
//			Data:    generateBinaryVectors(),
//			Dim:     dim,
//		}
//		insertData := storage.InsertData{
//			Data: data,
//			Infos: []storage.BlobInfo{
//				{
//					Length: 10,
//				},
//			},
//		}
//		binLogs, _, err := insertCodec.Serialize(999, 888, &insertData)
//		assert.NoError(t, err)
//		kvs := make(map[string][]byte, len(binLogs))
//		paths := make([]string, 0, len(binLogs))
//		for i, blob := range binLogs {
//			key := path.Join(binaryVectorBinlogPath, strconv.Itoa(i))
//			paths = append(paths, key)
//			kvs[key] = blob.Value[:]
//		}
//		err = in.chunkManager.MultiWrite(kvs)
//		assert.NoError(t, err)
//
//		indexMeta := &indexpb.IndexMeta{
//			IndexBuildID: indexBuildID2,
//			State:        commonpb.IndexState_InProgress,
//			IndexVersion: 1,
//		}
//
//		value, err := proto.Marshal(indexMeta)
//		assert.NoError(t, err)
//		err = in.etcdKV.Save(metaPath2, string(value))
//		assert.NoError(t, err)
//		req := &indexpb.CreateIndexRequest{
//			IndexBuildID: indexBuildID2,
//			IndexName:    "BinaryVector",
//			IndexID:      indexID,
//			Version:      1,
//			MetaPath:     metaPath2,
//			DataPaths:    paths,
//			TypeParams: []*commonpb.KeyValuePair{
//				{
//					Key:   common.DimKey,
//					Value: "8",
//				},
//			},
//			IndexParams: []*commonpb.KeyValuePair{
//				{
//					Key:   common.IndexTypeKey,
//					Value: "BIN_FLAT",
//				},
//				{
//					Key:   common.MetricTypeKey,
//					Value: "JACCARD",
//				},
//			},
//		}
//
//		status, err2 := in.CreateIndex(ctx, req)
//		assert.Nil(t, err2)
//		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
//
//		strValue, err3 := in.etcdKV.Load(metaPath2)
//		assert.Nil(t, err3)
//		indexMetaTmp := indexpb.IndexMeta{}
//		err = proto.Unmarshal([]byte(strValue), &indexMetaTmp)
//		assert.NoError(t, err)
//		for indexMetaTmp.State != commonpb.IndexState_Finished {
//			time.Sleep(100 * time.Millisecond)
//			strValue, err = in.etcdKV.Load(metaPath2)
//			assert.NoError(t, err)
//			err = proto.Unmarshal([]byte(strValue), &indexMetaTmp)
//			assert.NoError(t, err)
//		}
//		defer in.chunkManager.MultiRemove(indexMetaTmp.IndexFileKeys)
//		defer func() {
//			for k := range kvs {
//				err = in.chunkManager.Remove(k)
//				assert.NoError(t, err)
//			}
//		}()
//
//		defer in.etcdKV.RemoveWithPrefix(metaPath2)
//	})
//
//	t.Run("Create DeletedIndex", func(t *testing.T) {
//		var insertCodec storage.InsertCodec
//
//		insertCodec.Schema = &etcdpb.CollectionMeta{
//			ID: collectionID,
//			Schema: &schemapb.CollectionSchema{
//				Fields: []*schemapb.FieldSchema{
//					{
//						FieldID:      floatVectorFieldID,
//						Name:         floatVectorFieldName,
//						IsPrimaryKey: false,
//						DataType:     schemapb.DataType_FloatVector,
//					},
//				},
//			},
//		}
//		data := make(map[UniqueID]storage.FieldData)
//		tsData := make([]int64, nb)
//		for i := 0; i < nb; i++ {
//			tsData[i] = int64(i + 100)
//		}
//		data[tsFieldID] = &storage.Int64FieldData{
//			NumRows: []int64{nb},
//			Data:    tsData,
//		}
//		data[floatVectorFieldID] = &storage.FloatVectorFieldData{
//			NumRows: []int64{nb},
//			Data:    generateFloatVectors(),
//			Dim:     dim,
//		}
//		insertData := storage.InsertData{
//			Data: data,
//			Infos: []storage.BlobInfo{
//				{
//					Length: 10,
//				},
//			},
//		}
//		binLogs, _, err := insertCodec.Serialize(999, 888, &insertData)
//		assert.NoError(t, err)
//		kvs := make(map[string][]byte, len(binLogs))
//		paths := make([]string, 0, len(binLogs))
//		for i, blob := range binLogs {
//			key := path.Join(floatVectorBinlogPath, strconv.Itoa(i))
//			paths = append(paths, key)
//			kvs[key] = blob.Value[:]
//		}
//		err = in.chunkManager.MultiWrite(kvs)
//		assert.NoError(t, err)
//
//		indexMeta := &indexpb.IndexMeta{
//			IndexBuildID: indexBuildID1,
//			State:        commonpb.IndexState_InProgress,
//			IndexVersion: 1,
//			MarkDeleted:  true,
//		}
//
//		value, err := proto.Marshal(indexMeta)
//		assert.NoError(t, err)
//		err = in.etcdKV.Save(metaPath3, string(value))
//		assert.NoError(t, err)
//		req := &indexpb.CreateIndexRequest{
//			IndexBuildID: indexBuildID1,
//			IndexName:    "FloatVector",
//			IndexID:      indexID,
//			Version:      1,
//			MetaPath:     metaPath3,
//			DataPaths:    paths,
//			TypeParams: []*commonpb.KeyValuePair{
//				{
//					Key:   common.DimKey,
//					Value: "8",
//				},
//			},
//			IndexParams: []*commonpb.KeyValuePair{
//				{
//					Key:   common.IndexTypeKey,
//					Value: "IVF_SQ8",
//				},
//				{
//					Key:   common.IndexParamsKey,
//					Value: "{\"nlist\": 128}",
//				},
//				{
//					Key:   common.MetricTypeKey,
//					Value: "L2",
//				},
//			},
//		}
//
//		status, err2 := in.CreateIndex(ctx, req)
//		assert.Nil(t, err2)
//		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
//		time.Sleep(100 * time.Millisecond)
//		strValue, err3 := in.etcdKV.Load(metaPath3)
//		assert.Nil(t, err3)
//		indexMetaTmp := indexpb.IndexMeta{}
//		err = proto.Unmarshal([]byte(strValue), &indexMetaTmp)
//		assert.NoError(t, err)
//		assert.Equal(t, true, indexMetaTmp.MarkDeleted)
//		assert.Equal(t, int64(1), indexMetaTmp.IndexVersion)
//		//for indexMetaTmp.State != commonpb.IndexState_Finished {
//		//	time.Sleep(100 * time.Millisecond)
//		//	strValue, err := in.etcdKV.Load(metaPath3)
//		//	assert.NoError(t, err)
//		//	err = proto.Unmarshal([]byte(strValue), &indexMetaTmp)
//		//	assert.NoError(t, err)
//		//}
//		defer in.chunkManager.MultiRemove(indexMetaTmp.IndexFileKeys)
//		defer func() {
//			for k := range kvs {
//				err = in.chunkManager.Remove(k)
//				assert.NoError(t, err)
//			}
//		}()
//
//		defer in.etcdKV.RemoveWithPrefix(metaPath3)
//	})
//
//	t.Run("GetComponentStates", func(t *testing.T) {
//		resp, err := in.GetComponentStates(ctx)
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//		assert.Equal(t, commonpb.StateCode_Healthy, resp.State.StateCode)
//	})
//
//	t.Run("GetTimeTickChannel", func(t *testing.T) {
//		resp, err := in.GetTimeTickChannel(ctx)
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetStatisticsChannel", func(t *testing.T) {
//		resp, err := in.GetStatisticsChannel(ctx)
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	})
//
//	t.Run("ShowConfigurations", func(t *testing.T) {
//		pattern := "Port"
//		req := &internalpb.ShowConfigurationsRequest{
//			Base: &commonpb.MsgBase{
//				MsgType: commonpb.MsgType_WatchQueryChannels,
//				MsgID:   rand.Int63(),
//			},
//			Pattern: pattern,
//		}
//
//		resp, err := in.ShowConfigurations(ctx, req)
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//		assert.Equal(t, 1, len(resp.Configuations))
//		assert.Equal(t, "indexnode.port", resp.Configuations[0].Key)
//	})
//
//	t.Run("GetMetrics_system_info", func(t *testing.T) {
//		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
//		assert.NoError(t, err)
//		resp, err := in.GetMetrics(ctx, req)
//		assert.NoError(t, err)
//		log.Info("GetMetrics_system_info",
//			zap.String("resp", resp.Response),
//			zap.String("name", resp.ComponentName))
//	})
//	err = in.etcdKV.RemoveWithPrefix("session/IndexNode")
//	assert.NoError(t, err)
//
//	resp, err = getEtcdClient().Get(ctx, path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot, in.session.ServerName))
//	assert.NoError(t, err)
//	assert.Equal(t, resp.Count, int64(0))
//}

func TestComponentState(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in := NewIndexNode(ctx, factory)
	in.SetEtcdClient(getEtcdClient())
	state, err := in.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Abnormal)

	assert.Nil(t, in.Init())
	state, err = in.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Initializing)

	assert.Nil(t, in.Start())
	state, err = in.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Healthy)

	assert.Nil(t, in.Stop())
	assert.Nil(t, in.Stop())
	state, err = in.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Abnormal)
}

func TestGetTimeTickChannel(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in := NewIndexNode(ctx, factory)
	ret, err := in.GetTimeTickChannel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, ret.Status.ErrorCode, commonpb.ErrorCode_Success)
}

func TestGetStatisticChannel(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in := NewIndexNode(ctx, factory)

	ret, err := in.GetStatisticsChannel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, ret.Status.ErrorCode, commonpb.ErrorCode_Success)
}

func TestIndexTaskWhenStoppingNode(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in := NewIndexNode(ctx, factory)

	in.loadOrStoreTask("cluster-1", 1, &taskInfo{
		state: commonpb.IndexState_InProgress,
	})
	in.loadOrStoreTask("cluster-2", 2, &taskInfo{
		state: commonpb.IndexState_Finished,
	})

	assert.True(t, in.hasInProgressTask())
	go func() {
		time.Sleep(2 * time.Second)
		in.storeTaskState("cluster-1", 1, commonpb.IndexState_Finished, "")
	}()
	noTaskChan := make(chan struct{})
	go func() {
		in.waitTaskFinish()
		close(noTaskChan)
	}()
	select {
	case <-noTaskChan:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout task chan")
	}
}

func TestGetSetAddress(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in := NewIndexNode(ctx, factory)
	in.SetAddress("address")
	assert.Equal(t, "address", in.GetAddress())
}

func TestInitErr(t *testing.T) {
	// var (
	// 	factory = &mockFactory{}
	// 	ctx     = context.TODO()
	// )
	// in, err := NewIndexNode(ctx, factory)
	// assert.NoError(t, err)
	// in.SetEtcdClient(getEtcdClient())
	// assert.Error(t, in.Init())
}

func setup() {
	startEmbedEtcd()
}

func teardown() {
	stopEmbedEtcd()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
