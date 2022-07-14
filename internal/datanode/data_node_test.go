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

package datanode

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const returnError = "ReturnError"

type ctxKey struct{}

func TestMain(t *testing.M) {
	rand.Seed(time.Now().Unix())
	path := "/tmp/milvus_ut/rdb_data"
	os.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)

	Params.DataNodeCfg.InitAlias("datanode-alias-1")
	Params.Init()
	// change to specific channel for test
	Params.CommonCfg.DataCoordTimeTick = Params.CommonCfg.DataCoordTimeTick + strconv.Itoa(rand.Int())

	code := t.Run()
	os.Exit(code)
}

func TestDataNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	assert.Nil(t, err)
	err = node.Start()
	assert.Nil(t, err)
	defer node.Stop()

	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath("/tmp/lib/milvus"))
	Params.DataNodeCfg.SetNodeID(1)
	t.Run("Test WatchDmChannels ", func(t *testing.T) {
		emptyNode := &DataNode{}

		status, err := emptyNode.WatchDmChannels(ctx, &datapb.WatchDmChannelsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	t.Run("Test SetRootCoord", func(t *testing.T) {
		emptyDN := &DataNode{}
		tests := []struct {
			inrc        types.RootCoord
			isvalid     bool
			description string
		}{
			{nil, false, "nil input"},
			{&RootCoordFactory{}, true, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := emptyDN.SetRootCoord(test.inrc)
				if test.isvalid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("Test SetDataCoord", func(t *testing.T) {
		emptyDN := &DataNode{}
		tests := []struct {
			inrc        types.DataCoord
			isvalid     bool
			description string
		}{
			{nil, false, "nil input"},
			{&DataCoordFactory{}, true, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := emptyDN.SetDataCoord(test.inrc)
				if test.isvalid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("Test GetComponentStates", func(t *testing.T) {
		stat, err := node.GetComponentStates(node.ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.Status.ErrorCode)
	})

	t.Run("Test GetCompactionState", func(t *testing.T) {
		node.compactionExecutor.executing.Store(int64(3), 0)
		node.compactionExecutor.executing.Store(int64(2), 0)
		node.compactionExecutor.completed.Store(int64(1), &datapb.CompactionResult{
			PlanID:    1,
			SegmentID: 10,
		})
		stat, err := node.GetCompactionState(node.ctx, nil)
		assert.NoError(t, err)

		assert.Equal(t, 3, len(stat.GetResults()))

		cnt := 0
		for _, v := range stat.GetResults() {
			if v.GetState() == commonpb.CompactionState_Completed {
				cnt++
			}
		}
		assert.Equal(t, 1, cnt)

		cnt = 0
		node.compactionExecutor.completed.Range(func(k, v interface{}) bool {
			cnt++
			return true
		})
		assert.Equal(t, 0, cnt)
	})

	t.Run("Test GetCompactionState unhealthy", func(t *testing.T) {
		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		resp, _ := node.GetCompactionState(ctx, nil)
		assert.Equal(t, "DataNode is unhealthy", resp.GetStatus().GetReason())
		node.UpdateStateCode(internalpb.StateCode_Healthy)
	})

	t.Run("Test FlushSegments", func(t *testing.T) {
		dmChannelName := "fake-by-dev-rootcoord-dml-channel-test-FlushSegments"

		node1 := newIDLEDataNodeMock(context.TODO(), schemapb.DataType_Int64)
		node1.SetEtcdClient(etcdCli)
		err = node1.Init()
		assert.Nil(t, err)
		err = node1.Start()
		assert.Nil(t, err)
		defer func() {
			err := node1.Stop()
			assert.Nil(t, err)
		}()

		vchan := &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         dmChannelName,
			UnflushedSegmentIds: []int64{},
			FlushedSegmentIds:   []int64{},
		}

		err := node1.flowgraphManager.addAndStart(node1, vchan)
		require.Nil(t, err)

		fgservice, ok := node1.flowgraphManager.getFlowgraphService(dmChannelName)
		assert.True(t, ok)

		err = fgservice.replica.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       0,
			collID:      1,
			partitionID: 1,
			channelName: dmChannelName,
			startPos:    &internalpb.MsgPosition{},
			endPos:      &internalpb.MsgPosition{},
		})
		assert.Nil(t, err)

		req := &datapb.FlushSegmentsRequest{
			Base:         &commonpb.MsgBase{},
			DbID:         0,
			CollectionID: 1,
			SegmentIDs:   []int64{0},
		}

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			status, err := node1.FlushSegments(node1.ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		}()

		go func() {
			defer wg.Done()

			timeTickMsgPack := msgstream.MsgPack{}
			timeTickMsg := &msgstream.TimeTickMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: Timestamp(0),
					EndTimestamp:   Timestamp(0),
					HashValues:     []uint32{0},
				},
				TimeTickMsg: internalpb.TimeTickMsg{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_TimeTick,
						MsgID:     UniqueID(0),
						Timestamp: math.MaxUint64,
						SourceID:  0,
					},
				},
			}
			timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

			// pulsar produce
			factory := dependency.NewDefaultFactory(true)
			insertStream, err := factory.NewMsgStream(node1.ctx)
			assert.NoError(t, err)
			insertStream.AsProducer([]string{dmChannelName})
			insertStream.Start()
			defer insertStream.Close()

			err = insertStream.Broadcast(&timeTickMsgPack)
			assert.NoError(t, err)

			err = insertStream.Broadcast(&timeTickMsgPack)
			assert.NoError(t, err)
		}()

		wg.Wait()
		// dup call
		status, err := node1.FlushSegments(node1.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		// failure call
		req = &datapb.FlushSegmentsRequest{
			Base:         &commonpb.MsgBase{},
			DbID:         0,
			CollectionID: 1,
			SegmentIDs:   []int64{1},
		}

		status, err = node1.FlushSegments(node1.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

		req = &datapb.FlushSegmentsRequest{
			Base:           &commonpb.MsgBase{},
			DbID:           0,
			CollectionID:   1,
			SegmentIDs:     []int64{},
			MarkSegmentIDs: []int64{2},
		}

		status, err = node1.FlushSegments(node1.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	t.Run("Test GetTimeTickChannel", func(t *testing.T) {
		_, err := node.GetTimeTickChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test GetStatisticsChannel", func(t *testing.T) {
		_, err := node.GetStatisticsChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test getSystemInfoMetrics", func(t *testing.T) {
		emptyNode := &DataNode{}
		emptyNode.session = &sessionutil.Session{ServerID: 1}

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := emptyNode.getSystemInfoMetrics(context.TODO(), req)
		assert.NoError(t, err)
		log.Info("Test DataNode.getSystemInfoMetrics",
			zap.String("name", resp.ComponentName),
			zap.String("response", resp.Response))
	})

	t.Run("Test GetMetrics", func(t *testing.T) {
		node := &DataNode{}
		node.session = &sessionutil.Session{ServerID: 1}
		// server is closed
		node.State.Store(internalpb.StateCode_Abnormal)
		resp, err := node.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		node.State.Store(internalpb.StateCode_Healthy)

		// failed to parse metric type
		invalidRequest := "invalid request"
		resp, err = node.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Request: invalidRequest,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// unsupported metric type
		unsupportedMetricType := "unsupported"
		req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
		assert.NoError(t, err)
		resp, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// normal case
		req, err = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err = node.GetMetrics(node.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		log.Info("Test DataNode.GetMetrics",
			zap.String("name", resp.ComponentName),
			zap.String("response", resp.Response))
	})

	t.Run("Test Import", func(t *testing.T) {
		node.rootCoord = &RootCoordFactory{
			collectionID: 100,
			pkType:       schemapb.DataType_Int64,
		}
		content := []byte(`{
		"rows":[
			{"bool_field": true, "int8_field": 10, "int16_field": 101, "int32_field": 1001, "int64_field": 10001, "float32_field": 3.14, "float64_field": 1.56, "varChar_field": "hello world", "binary_vector_field": [254, 0, 254, 0], "float_vector_field": [1.1, 1.2]},
			{"bool_field": false, "int8_field": 11, "int16_field": 102, "int32_field": 1002, "int64_field": 10002, "float32_field": 3.15, "float64_field": 2.56, "varChar_field": "hello world", "binary_vector_field": [253, 0, 253, 0], "float_vector_field": [2.1, 2.2]},
			{"bool_field": true, "int8_field": 12, "int16_field": 103, "int32_field": 1003, "int64_field": 10003, "float32_field": 3.16, "float64_field": 3.56, "varChar_field": "hello world", "binary_vector_field": [252, 0, 252, 0], "float_vector_field": [3.1, 3.2]},
			{"bool_field": false, "int8_field": 13, "int16_field": 104, "int32_field": 1004, "int64_field": 10004, "float32_field": 3.17, "float64_field": 4.56, "varChar_field": "hello world", "binary_vector_field": [251, 0, 251, 0], "float_vector_field": [4.1, 4.2]},
			{"bool_field": true, "int8_field": 14, "int16_field": 105, "int32_field": 1005, "int64_field": 10005, "float32_field": 3.18, "float64_field": 5.56, "varChar_field": "hello world", "binary_vector_field": [250, 0, 250, 0], "float_vector_field": [5.1, 5.2]}
		]
		}`)

		filePath := "import/rows_1.json"
		err = node.chunkManager.Write(filePath, content)
		assert.NoError(t, err)
		req := &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
				ChannelNames: []string{"ch1", "ch2"},
				Files:        []string{filePath},
				RowBased:     true,
			},
		}
		node.rootCoord.(*RootCoordFactory).ReportImportErr = true
		_, err := node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		node.rootCoord.(*RootCoordFactory).ReportImportErr = false

		node.rootCoord.(*RootCoordFactory).ReportImportNotSuccess = true
		_, err = node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		node.rootCoord.(*RootCoordFactory).ReportImportNotSuccess = false

		node.dataCoord.(*DataCoordFactory).AddSegmentError = true
		_, err = node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		node.dataCoord.(*DataCoordFactory).AddSegmentError = false

		node.dataCoord.(*DataCoordFactory).AddSegmentNotSuccess = true
		_, err = node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		node.dataCoord.(*DataCoordFactory).AddSegmentNotSuccess = false

		stat, err := node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.GetErrorCode())
		assert.Equal(t, "", stat.GetReason())
	})

	t.Run("Test Import bad flow graph", func(t *testing.T) {
		node.rootCoord = &RootCoordFactory{
			collectionID: 100,
			pkType:       schemapb.DataType_Int64,
		}

		chName1 := "fake-by-dev-rootcoord-dml-testimport-1"
		chName2 := "fake-by-dev-rootcoord-dml-testimport-2"
		err := node.flowgraphManager.addAndStart(node, &datapb.VchannelInfo{
			CollectionID:        100,
			ChannelName:         chName1,
			UnflushedSegmentIds: []int64{},
			FlushedSegmentIds:   []int64{},
		})
		require.Nil(t, err)
		err = node.flowgraphManager.addAndStart(node, &datapb.VchannelInfo{
			CollectionID:        999, // wrong collection ID.
			ChannelName:         chName2,
			UnflushedSegmentIds: []int64{},
			FlushedSegmentIds:   []int64{},
		})
		require.Nil(t, err)

		_, ok := node.flowgraphManager.getFlowgraphService(chName1)
		assert.True(t, ok)
		_, ok = node.flowgraphManager.getFlowgraphService(chName2)
		assert.True(t, ok)

		content := []byte(`{
		"rows":[
			{"bool_field": true, "int8_field": 10, "int16_field": 101, "int32_field": 1001, "int64_field": 10001, "float32_field": 3.14, "float64_field": 1.56, "varChar_field": "hello world", "binary_vector_field": [254, 0, 254, 0], "float_vector_field": [1.1, 1.2]},
			{"bool_field": false, "int8_field": 11, "int16_field": 102, "int32_field": 1002, "int64_field": 10002, "float32_field": 3.15, "float64_field": 2.56, "varChar_field": "hello world", "binary_vector_field": [253, 0, 253, 0], "float_vector_field": [2.1, 2.2]},
			{"bool_field": true, "int8_field": 12, "int16_field": 103, "int32_field": 1003, "int64_field": 10003, "float32_field": 3.16, "float64_field": 3.56, "varChar_field": "hello world", "binary_vector_field": [252, 0, 252, 0], "float_vector_field": [3.1, 3.2]},
			{"bool_field": false, "int8_field": 13, "int16_field": 104, "int32_field": 1004, "int64_field": 10004, "float32_field": 3.17, "float64_field": 4.56, "varChar_field": "hello world", "binary_vector_field": [251, 0, 251, 0], "float_vector_field": [4.1, 4.2]},
			{"bool_field": true, "int8_field": 14, "int16_field": 105, "int32_field": 1005, "int64_field": 10005, "float32_field": 3.18, "float64_field": 5.56, "varChar_field": "hello world", "binary_vector_field": [250, 0, 250, 0], "float_vector_field": [5.1, 5.2]}
		]
		}`)

		filePath := "import/rows_1.json"
		err = node.chunkManager.Write(filePath, content)
		assert.NoError(t, err)
		req := &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
				ChannelNames: []string{chName1, chName2},
				Files:        []string{filePath},
				RowBased:     true,
			},
		}
		stat, err := node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.GetErrorCode())
		assert.Equal(t, "", stat.GetReason())
	})

	t.Run("Test Import report import error", func(t *testing.T) {
		node.rootCoord = &RootCoordFactory{
			collectionID: 100,
			pkType:       schemapb.DataType_Int64,
		}
		content := []byte(`{
		"rows":[
			{"bool_field": true, "int8_field": 10, "int16_field": 101, "int32_field": 1001, "int64_field": 10001, "float32_field": 3.14, "float64_field": 1.56, "varChar_field": "hello world", "binary_vector_field": [254, 0, 254, 0], "float_vector_field": [1.1, 1.2]},
			{"bool_field": false, "int8_field": 11, "int16_field": 102, "int32_field": 1002, "int64_field": 10002, "float32_field": 3.15, "float64_field": 2.56, "varChar_field": "hello world", "binary_vector_field": [253, 0, 253, 0], "float_vector_field": [2.1, 2.2]},
			{"bool_field": true, "int8_field": 12, "int16_field": 103, "int32_field": 1003, "int64_field": 10003, "float32_field": 3.16, "float64_field": 3.56, "varChar_field": "hello world", "binary_vector_field": [252, 0, 252, 0], "float_vector_field": [3.1, 3.2]},
			{"bool_field": false, "int8_field": 13, "int16_field": 104, "int32_field": 1004, "int64_field": 10004, "float32_field": 3.17, "float64_field": 4.56, "varChar_field": "hello world", "binary_vector_field": [251, 0, 251, 0], "float_vector_field": [4.1, 4.2]},
			{"bool_field": true, "int8_field": 14, "int16_field": 105, "int32_field": 1005, "int64_field": 10005, "float32_field": 3.18, "float64_field": 5.56, "varChar_field": "hello world", "binary_vector_field": [250, 0, 250, 0], "float_vector_field": [5.1, 5.2]}
		]
		}`)

		filePath := "import/rows_1.json"
		err = node.chunkManager.Write(filePath, content)
		assert.NoError(t, err)
		req := &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
				ChannelNames: []string{"ch1", "ch2"},
				Files:        []string{filePath},
				RowBased:     true,
			},
		}
		stat, err := node.Import(context.WithValue(node.ctx, ctxKey{}, returnError), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stat.GetErrorCode())
	})

	t.Run("Test Import error", func(t *testing.T) {
		node.rootCoord = &RootCoordFactory{collectionID: -1}
		req := &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
			},
		}
		stat, err := node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stat.ErrorCode)

		stat, err = node.Import(context.WithValue(ctx, ctxKey{}, returnError), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stat.GetErrorCode())

		node.State.Store(internalpb.StateCode_Abnormal)
		stat, err = node.Import(context.WithValue(ctx, ctxKey{}, ""), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stat.GetErrorCode())
	})

	t.Run("Test Import callback func error", func(t *testing.T) {
		req := &datapb.ImportTaskRequest{
			ImportTask: &datapb.ImportTask{
				CollectionId: 100,
				PartitionId:  100,
				ChannelNames: []string{"ch1", "ch2"},
			},
		}
		importResult := &rootcoordpb.ImportResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			TaskId:     0,
			DatanodeId: 0,
			State:      commonpb.ImportState_ImportStarted,
			Segments:   make([]int64, 0),
			AutoIds:    make([]int64, 0),
			RowCount:   0,
		}
		callback := importFlushReqFunc(node, req, importResult, nil, 0)
		err := callback(nil, len(req.ImportTask.ChannelNames)+1)
		assert.Error(t, err)
	})

	t.Run("Test BackGroundGC", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)

		vchanNameCh := make(chan string)
		node.clearSignal = vchanNameCh
		go node.BackGroundGC(vchanNameCh)

		testDataSyncs := []struct {
			dmChannelName string
		}{
			{"fake-by-dev-rootcoord-dml-backgroundgc-1"},
			{"fake-by-dev-rootcoord-dml-backgroundgc-2"},
			{"fake-by-dev-rootcoord-dml-backgroundgc-3"},
			{""},
			{""},
		}

		for i, test := range testDataSyncs {
			if i <= 2 {
				err = node.flowgraphManager.addAndStart(node, &datapb.VchannelInfo{CollectionID: 1, ChannelName: test.dmChannelName})
				assert.Nil(t, err)
				vchanNameCh <- test.dmChannelName
			}
		}
		cancel()
	})
}

func TestDataNode_AddSegment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	assert.Nil(t, err)
	err = node.Start()
	assert.Nil(t, err)
	defer node.Stop()

	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath("/tmp/lib/milvus"))
	Params.DataNodeCfg.SetNodeID(1)

	t.Run("test AddSegment", func(t *testing.T) {
		node.rootCoord = &RootCoordFactory{
			collectionID: 100,
			pkType:       schemapb.DataType_Int64,
		}

		chName1 := "fake-by-dev-rootcoord-dml-testaddsegment-1"
		chName2 := "fake-by-dev-rootcoord-dml-testaddsegment-2"
		err := node.flowgraphManager.addAndStart(node, &datapb.VchannelInfo{
			CollectionID:        100,
			ChannelName:         chName1,
			UnflushedSegmentIds: []int64{},
			FlushedSegmentIds:   []int64{},
		})
		require.Nil(t, err)
		err = node.flowgraphManager.addAndStart(node, &datapb.VchannelInfo{
			CollectionID:        100,
			ChannelName:         chName2,
			UnflushedSegmentIds: []int64{},
			FlushedSegmentIds:   []int64{},
		})
		require.Nil(t, err)

		_, ok := node.flowgraphManager.getFlowgraphService(chName1)
		assert.True(t, ok)
		_, ok = node.flowgraphManager.getFlowgraphService(chName2)
		assert.True(t, ok)

		stat, err := node.AddImportSegment(context.WithValue(ctx, ctxKey{}, ""), &datapb.AddImportSegmentRequest{
			SegmentId:    100,
			CollectionId: 100,
			PartitionId:  100,
			ChannelName:  chName1,
			RowNum:       500,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.GetErrorCode())
		assert.Equal(t, "", stat.GetReason())

		addImportSegmentAttempts = 3
		stat, err = node.AddImportSegment(context.WithValue(ctx, ctxKey{}, ""), &datapb.AddImportSegmentRequest{
			SegmentId:    100,
			CollectionId: 100,
			PartitionId:  100,
			ChannelName:  "bad-ch-name",
			RowNum:       500,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stat.GetErrorCode())
	})
}

func TestWatchChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	assert.Nil(t, err)
	err = node.Start()
	assert.Nil(t, err)
	defer node.Stop()
	err = node.Register()
	assert.Nil(t, err)

	defer cancel()

	t.Run("test watch channel", func(t *testing.T) {
		// GOOSE TODO
		kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
		oldInvalidCh := "datanode-etcd-test-by-dev-rootcoord-dml-channel-invalid"
		path := fmt.Sprintf("%s/%d/%s", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID(), oldInvalidCh)
		err = kv.Save(path, string([]byte{23}))
		assert.NoError(t, err)

		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		path = fmt.Sprintf("%s/%d/%s", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID(), ch)
		c := make(chan struct{})
		go func() {
			ec := kv.WatchWithPrefix(fmt.Sprintf("%s/%d", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID()))
			c <- struct{}{}
			cnt := 0
			for {
				evt := <-ec
				for _, event := range evt.Events {
					if strings.Contains(string(event.Kv.Key), ch) {
						cnt++
					}
				}
				if cnt >= 2 {
					break
				}
			}
			c <- struct{}{}
		}()
		// wait for check goroutine start Watch
		<-c

		vchan := &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         ch,
			UnflushedSegmentIds: []int64{},
		}
		info := &datapb.ChannelWatchInfo{
			State:     datapb.ChannelWatchState_ToWatch,
			Vchan:     vchan,
			TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
		}
		val, err := proto.Marshal(info)
		assert.Nil(t, err)
		err = kv.Save(path, string(val))
		assert.Nil(t, err)

		// wait for check goroutine received 2 events
		<-c
		exist := node.flowgraphManager.exist(ch)
		assert.True(t, exist)

		err = kv.RemoveWithPrefix(fmt.Sprintf("%s/%d", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID()))
		assert.Nil(t, err)
		//TODO there is not way to sync Release done, use sleep for now
		time.Sleep(100 * time.Millisecond)

		exist = node.flowgraphManager.exist(ch)
		assert.False(t, exist)
	})

	t.Run("Test release channel", func(t *testing.T) {
		kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
		oldInvalidCh := "datanode-etcd-test-by-dev-rootcoord-dml-channel-invalid"
		path := fmt.Sprintf("%s/%d/%s", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID(), oldInvalidCh)
		err = kv.Save(path, string([]byte{23}))
		assert.NoError(t, err)

		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		path = fmt.Sprintf("%s/%d/%s", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID(), ch)
		c := make(chan struct{})
		go func() {
			ec := kv.WatchWithPrefix(fmt.Sprintf("%s/%d", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID()))
			c <- struct{}{}
			cnt := 0
			for {
				evt := <-ec
				for _, event := range evt.Events {
					if strings.Contains(string(event.Kv.Key), ch) {
						cnt++
					}
				}
				if cnt >= 2 {
					break
				}
			}
			c <- struct{}{}
		}()
		// wait for check goroutine start Watch
		<-c

		vchan := &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         ch,
			UnflushedSegmentIds: []int64{},
		}
		info := &datapb.ChannelWatchInfo{
			State:     datapb.ChannelWatchState_ToRelease,
			Vchan:     vchan,
			TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
		}
		val, err := proto.Marshal(info)
		assert.Nil(t, err)
		err = kv.Save(path, string(val))
		assert.Nil(t, err)

		// wait for check goroutine received 2 events
		<-c
		exist := node.flowgraphManager.exist(ch)
		assert.False(t, exist)

		err = kv.RemoveWithPrefix(fmt.Sprintf("%s/%d", Params.DataNodeCfg.ChannelWatchSubPath, Params.DataNodeCfg.GetNodeID()))
		assert.Nil(t, err)
		//TODO there is not way to sync Release done, use sleep for now
		time.Sleep(100 * time.Millisecond)

		exist = node.flowgraphManager.exist(ch)
		assert.False(t, exist)

	})

	t.Run("handle watch info failed", func(t *testing.T) {
		e := &event{
			eventType: putEventType,
		}

		node.handleWatchInfo(e, "test1", []byte{23})

		exist := node.flowgraphManager.exist("test1")
		assert.False(t, exist)

		info := datapb.ChannelWatchInfo{
			Vchan: nil,
			State: datapb.ChannelWatchState_Uncomplete,
		}
		bs, err := proto.Marshal(&info)
		assert.NoError(t, err)
		node.handleWatchInfo(e, "test2", bs)

		exist = node.flowgraphManager.exist("test2")
		assert.False(t, exist)

		chPut := make(chan struct{}, 1)
		chDel := make(chan struct{}, 1)

		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		m := newChannelEventManager(
			func(info *datapb.ChannelWatchInfo, version int64) error {
				r := node.handlePutEvent(info, version)
				chPut <- struct{}{}
				return r
			},
			func(vChan string) {
				node.handleDeleteEvent(vChan)
				chDel <- struct{}{}
			}, time.Millisecond*100,
		)
		node.eventManagerMap.Store(ch, m)
		m.Run()

		info = datapb.ChannelWatchInfo{
			Vchan:     &datapb.VchannelInfo{ChannelName: ch},
			State:     datapb.ChannelWatchState_Uncomplete,
			TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
		}
		bs, err = proto.Marshal(&info)
		assert.NoError(t, err)

		msFactory := node.factory
		defer func() { node.factory = msFactory }()

		node.factory = &FailMessageStreamFactory{}
		node.handleWatchInfo(e, ch, bs)
		<-chPut
		exist = node.flowgraphManager.exist(ch)
		assert.False(t, exist)
	})

	t.Run("handle watchinfo out of date", func(t *testing.T) {
		chPut := make(chan struct{}, 1)
		chDel := make(chan struct{}, 1)
		// inject eventManager
		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		m := newChannelEventManager(
			func(info *datapb.ChannelWatchInfo, version int64) error {
				r := node.handlePutEvent(info, version)
				chPut <- struct{}{}
				return r
			},
			func(vChan string) {
				node.handleDeleteEvent(vChan)
				chDel <- struct{}{}
			}, time.Millisecond*100,
		)
		node.eventManagerMap.Store(ch, m)
		m.Run()
		e := &event{
			eventType: putEventType,
			version:   10000,
		}

		info := datapb.ChannelWatchInfo{
			Vchan:     &datapb.VchannelInfo{ChannelName: ch},
			State:     datapb.ChannelWatchState_Uncomplete,
			TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
		}
		bs, err := proto.Marshal(&info)
		assert.NoError(t, err)

		node.handleWatchInfo(e, ch, bs)
		<-chPut
		exist := node.flowgraphManager.exist("test3")
		assert.False(t, exist)
	})

	t.Run("handle watchinfo compatibility", func(t *testing.T) {
		info := datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID:        1,
				ChannelName:         "delta-channel1",
				UnflushedSegments:   []*datapb.SegmentInfo{{ID: 1}},
				FlushedSegments:     []*datapb.SegmentInfo{{ID: 2}},
				DroppedSegments:     []*datapb.SegmentInfo{{ID: 3}},
				UnflushedSegmentIds: []int64{1},
			},
			State:     datapb.ChannelWatchState_Uncomplete,
			TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
		}
		bs, err := proto.Marshal(&info)
		assert.NoError(t, err)

		newWatchInfo, err := parsePutEventData(bs)
		assert.NoError(t, err)

		assert.Equal(t, []*datapb.SegmentInfo{}, newWatchInfo.GetVchan().GetUnflushedSegments())
		assert.Equal(t, []*datapb.SegmentInfo{}, newWatchInfo.GetVchan().GetFlushedSegments())
		assert.Equal(t, []*datapb.SegmentInfo{}, newWatchInfo.GetVchan().GetDroppedSegments())
		assert.NotEmpty(t, newWatchInfo.GetVchan().GetUnflushedSegmentIds())
		assert.NotEmpty(t, newWatchInfo.GetVchan().GetFlushedSegmentIds())
		assert.NotEmpty(t, newWatchInfo.GetVchan().GetDroppedSegmentIds())
	})
}

func TestDataNode_GetComponentStates(t *testing.T) {
	n := &DataNode{}
	n.State.Store(internalpb.StateCode_Healthy)
	resp, err := n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	n.session = &sessionutil.Session{}
	n.session.UpdateRegistered(true)
	resp, err = n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestDataNode_ResendSegmentStats(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	dmChannelName := "fake-by-dev-rootcoord-dml-channel-test-ResendSegmentStats"

	node := newIDLEDataNodeMock(context.TODO(), schemapb.DataType_Int64)
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	assert.Nil(t, err)
	err = node.Start()
	assert.Nil(t, err)
	defer func() {
		err := node.Stop()
		assert.Nil(t, err)
	}()

	vChan := &datapb.VchannelInfo{
		CollectionID:        1,
		ChannelName:         dmChannelName,
		UnflushedSegmentIds: []int64{},
		FlushedSegmentIds:   []int64{},
	}

	err = node.flowgraphManager.addAndStart(node, vChan)
	require.Nil(t, err)

	fgService, ok := node.flowgraphManager.getFlowgraphService(dmChannelName)
	assert.True(t, ok)

	err = fgService.replica.addSegment(addSegmentReq{
		segType:     datapb.SegmentType_New,
		segID:       0,
		collID:      1,
		partitionID: 1,
		channelName: dmChannelName,
		startPos:    &internalpb.MsgPosition{},
		endPos:      &internalpb.MsgPosition{},
	})
	assert.Nil(t, err)
	err = fgService.replica.addSegment(addSegmentReq{
		segType:     datapb.SegmentType_New,
		segID:       1,
		collID:      1,
		partitionID: 2,
		channelName: dmChannelName,
		startPos:    &internalpb.MsgPosition{},
		endPos:      &internalpb.MsgPosition{},
	})
	assert.Nil(t, err)
	err = fgService.replica.addSegment(addSegmentReq{
		segType:     datapb.SegmentType_New,
		segID:       2,
		collID:      1,
		partitionID: 3,
		channelName: dmChannelName,
		startPos:    &internalpb.MsgPosition{},
		endPos:      &internalpb.MsgPosition{},
	})
	assert.Nil(t, err)

	req := &datapb.ResendSegmentStatsRequest{
		Base: &commonpb.MsgBase{},
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	resp, err := node.ResendSegmentStats(node.ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.ElementsMatch(t, []UniqueID{0, 1, 2}, resp.GetSegResent())

	// Duplicate call.
	resp, err = node.ResendSegmentStats(node.ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.ElementsMatch(t, []UniqueID{0, 1, 2}, resp.GetSegResent())
}
