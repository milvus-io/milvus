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

package msgdispatcher

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestManager(t *testing.T) {
	t.Run("test add and remove dispatcher", func(t *testing.T) {
		c := NewDispatcherManager("mock_pchannel_0", typeutil.ProxyRole, 1, newMockFactory())
		assert.NotNil(t, c)
		assert.Equal(t, 0, c.NumConsumer())
		assert.Equal(t, 0, c.NumTarget())

		var offset int
		for i := 0; i < 100; i++ {
			r := rand.Intn(10) + 1
			for j := 0; j < r; j++ {
				offset++
				vchannel := fmt.Sprintf("mock-pchannel-dml_0_vchannelv%d", offset)
				t.Logf("add vchannel, %s", vchannel)
				_, err := c.Add(context.Background(), NewStreamConfig(vchannel, nil, common.SubscriptionPositionUnknown))
				assert.NoError(t, err)
				assert.Equal(t, offset, c.NumConsumer())
				assert.Equal(t, offset, c.NumTarget())
			}
			for j := 0; j < rand.Intn(r); j++ {
				vchannel := fmt.Sprintf("mock-pchannel-dml_0_vchannelv%d", offset)
				t.Logf("remove vchannel, %s", vchannel)
				c.Remove(vchannel)
				offset--
				assert.Equal(t, offset, c.NumConsumer())
				assert.Equal(t, offset, c.NumTarget())
			}
		}
	})

	t.Run("test merge and split", func(t *testing.T) {
		prefix := fmt.Sprintf("mock%d", time.Now().UnixNano())
		ctx := context.Background()
		c := NewDispatcherManager(prefix+"_pchannel_0", typeutil.ProxyRole, 1, newMockFactory())
		assert.NotNil(t, c)
		_, err := c.Add(ctx, NewStreamConfig("mock_vchannel_0", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_2", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		assert.Equal(t, 3, c.NumConsumer())
		assert.Equal(t, 3, c.NumTarget())
		c.(*dispatcherManager).mainDispatcher.curTs.Store(1000)
		c.(*dispatcherManager).mu.RLock()
		for _, d := range c.(*dispatcherManager).soloDispatchers {
			d.curTs.Store(1000)
		}
		c.(*dispatcherManager).mu.RUnlock()

		c.(*dispatcherManager).tryMerge()
		assert.Equal(t, 1, c.NumConsumer())
		assert.Equal(t, 3, c.NumTarget())

		info := &target{
			vchannel: "mock_vchannel_2",
			pos:      nil,
			ch:       nil,
		}
		c.(*dispatcherManager).split(info)
		assert.Equal(t, 2, c.NumConsumer())
	})

	t.Run("test run and close", func(t *testing.T) {
		prefix := fmt.Sprintf("mock%d", time.Now().UnixNano())
		ctx := context.Background()
		c := NewDispatcherManager(prefix+"_pchannel_0", typeutil.ProxyRole, 1, newMockFactory())
		assert.NotNil(t, c)
		_, err := c.Add(ctx, NewStreamConfig("mock_vchannel_0", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_2", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		assert.Equal(t, 3, c.NumConsumer())
		assert.Equal(t, 3, c.NumTarget())
		c.(*dispatcherManager).mainDispatcher.curTs.Store(1000)
		c.(*dispatcherManager).mu.RLock()
		for _, d := range c.(*dispatcherManager).soloDispatchers {
			d.curTs.Store(1000)
		}
		c.(*dispatcherManager).mu.RUnlock()

		checkIntervalK := paramtable.Get().MQCfg.MergeCheckInterval.Key
		paramtable.Get().Save(checkIntervalK, "0.01")
		defer paramtable.Get().Reset(checkIntervalK)
		go c.Run()
		assert.Eventually(t, func() bool {
			return c.NumConsumer() == 1 // expected merged
		}, 3*time.Second, 10*time.Millisecond)
		assert.Equal(t, 3, c.NumTarget())

		assert.NotPanics(t, func() {
			c.Close()
		})
	})

	t.Run("test add timeout", func(t *testing.T) {
		prefix := fmt.Sprintf("mock%d", time.Now().UnixNano())
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*2)
		defer cancel()
		time.Sleep(time.Millisecond * 2)
		c := NewDispatcherManager(prefix+"_pchannel_0", typeutil.ProxyRole, 1, newMockFactory())
		go c.Run()
		assert.NotNil(t, c)
		_, err := c.Add(ctx, NewStreamConfig("mock_vchannel_0", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_2", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		assert.Equal(t, 0, c.NumConsumer())
		assert.Equal(t, 0, c.NumTarget())

		assert.NotPanics(t, func() {
			c.Close()
		})
	})

	t.Run("test_repeated_vchannel", func(t *testing.T) {
		prefix := fmt.Sprintf("mock%d", time.Now().UnixNano())
		c := NewDispatcherManager(prefix+"_pchannel_0", typeutil.ProxyRole, 1, newMockFactory())
		go c.Run()
		assert.NotNil(t, c)
		ctx := context.Background()
		_, err := c.Add(ctx, NewStreamConfig("mock_vchannel_0", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_2", nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)

		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_0", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		_, err = c.Add(ctx, NewStreamConfig("mock_vchannel_2", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)

		assert.NotPanics(t, func() {
			c.Close()
		})
	})
}

type vchannelHelper struct {
	output <-chan *msgstream.MsgPack

	pubInsMsgNum int
	pubDelMsgNum int
	pubDDLMsgNum int
	pubPackNum   int

	subInsMsgNum int
	subDelMsgNum int
	subDDLMsgNum int
	subPackNum   int
}

type SimulationSuite struct {
	suite.Suite

	testVchannelNum int

	manager   DispatcherManager
	pchannel  string
	vchannels map[string]*vchannelHelper

	producer msgstream.MsgStream
	factory  msgstream.Factory
}

func (suite *SimulationSuite) SetupSuite() {
	suite.factory = newMockFactory()
}

func (suite *SimulationSuite) SetupTest() {
	suite.pchannel = fmt.Sprintf("by-dev-rootcoord-dispatcher-simulation-dml_%d", time.Now().UnixNano())
	producer, err := newMockProducer(suite.factory, suite.pchannel)
	assert.NoError(suite.T(), err)
	suite.producer = producer

	suite.manager = NewDispatcherManager(suite.pchannel, typeutil.DataNodeRole, 0, suite.factory)
	go suite.manager.Run()
}

func (suite *SimulationSuite) produceMsg(wg *sync.WaitGroup, collectionID int64) {
	defer wg.Done()

	const timeTickCount = 100
	var uniqueMsgID int64
	vchannelKeys := reflect.ValueOf(suite.vchannels).MapKeys()

	for i := 1; i <= timeTickCount; i++ {
		// produce random insert
		insNum := rand.Intn(10)
		for j := 0; j < insNum; j++ {
			vchannel := vchannelKeys[rand.Intn(len(vchannelKeys))].Interface().(string)
			err := suite.producer.Produce(context.TODO(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{genInsertMsg(rand.Intn(20)+1, vchannel, uniqueMsgID)},
			})
			assert.NoError(suite.T(), err)
			uniqueMsgID++
			suite.vchannels[vchannel].pubInsMsgNum++
		}
		// produce random delete
		delNum := rand.Intn(2)
		for j := 0; j < delNum; j++ {
			vchannel := vchannelKeys[rand.Intn(len(vchannelKeys))].Interface().(string)
			err := suite.producer.Produce(context.TODO(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{genDeleteMsg(rand.Intn(20)+1, vchannel, uniqueMsgID)},
			})
			assert.NoError(suite.T(), err)
			uniqueMsgID++
			suite.vchannels[vchannel].pubDelMsgNum++
		}
		// produce random ddl
		ddlNum := rand.Intn(2)
		for j := 0; j < ddlNum; j++ {
			err := suite.producer.Produce(context.TODO(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{genDDLMsg(commonpb.MsgType_DropCollection, collectionID)},
			})
			assert.NoError(suite.T(), err)
			for k := range suite.vchannels {
				suite.vchannels[k].pubDDLMsgNum++
			}
		}
		// produce time tick
		ts := uint64(i * 100)
		err := suite.producer.Produce(context.TODO(), &msgstream.MsgPack{
			Msgs: []msgstream.TsMsg{genTimeTickMsg(ts)},
		})
		assert.NoError(suite.T(), err)
		for k := range suite.vchannels {
			suite.vchannels[k].pubPackNum++
		}
	}
	suite.T().Logf("[%s] produce %d msgPack for %s done", time.Now(), timeTickCount, suite.pchannel)
}

func (suite *SimulationSuite) consumeMsg(ctx context.Context, wg *sync.WaitGroup, vchannel string) {
	defer wg.Done()
	var lastTs typeutil.Timestamp
	for {
		select {
		case <-ctx.Done():
			return
		case pack := <-suite.vchannels[vchannel].output:
			assert.Greater(suite.T(), pack.EndTs, lastTs)
			lastTs = pack.EndTs
			helper := suite.vchannels[vchannel]
			helper.subPackNum++
			for _, msg := range pack.Msgs {
				switch msg.Type() {
				case commonpb.MsgType_Insert:
					helper.subInsMsgNum++
				case commonpb.MsgType_Delete:
					helper.subDelMsgNum++
				case commonpb.MsgType_CreateCollection, commonpb.MsgType_DropCollection,
					commonpb.MsgType_CreatePartition, commonpb.MsgType_DropPartition:
					helper.subDDLMsgNum++
				}
			}
		}
	}
}

func (suite *SimulationSuite) produceTimeTickOnly(ctx context.Context) {
	tt := 1
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts := uint64(tt * 1000)
			err := suite.producer.Produce(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{genTimeTickMsg(ts)},
			})
			assert.NoError(suite.T(), err)
			tt++
		}
	}
}

func (suite *SimulationSuite) TestDispatchToVchannels() {
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()

	const (
		vchannelNum        = 10
		collectionID int64 = 1234
	)
	suite.vchannels = make(map[string]*vchannelHelper, vchannelNum)
	for i := 0; i < vchannelNum; i++ {
		vchannel := fmt.Sprintf("%s_%dv%d", suite.pchannel, collectionID, i)
		output, err := suite.manager.Add(context.Background(), NewStreamConfig(vchannel, nil, common.SubscriptionPositionEarliest))
		assert.NoError(suite.T(), err)
		suite.vchannels[vchannel] = &vchannelHelper{output: output}
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go suite.produceMsg(wg, collectionID)
	wg.Wait()
	for vchannel := range suite.vchannels {
		wg.Add(1)
		go suite.consumeMsg(ctx, wg, vchannel)
	}
	wg.Wait()
	for vchannel, helper := range suite.vchannels {
		msg := fmt.Sprintf("vchannel=%s", vchannel)
		assert.Equal(suite.T(), helper.pubInsMsgNum, helper.subInsMsgNum, msg)
		assert.Equal(suite.T(), helper.pubDelMsgNum, helper.subDelMsgNum, msg)
		assert.Equal(suite.T(), helper.pubDDLMsgNum, helper.subDDLMsgNum, msg)
		assert.Equal(suite.T(), helper.pubPackNum, helper.subPackNum, msg)
	}
}

func (suite *SimulationSuite) TestMerge() {
	ctx, cancel := context.WithCancel(context.Background())
	go suite.produceTimeTickOnly(ctx)

	const vchannelNum = 10
	suite.vchannels = make(map[string]*vchannelHelper, vchannelNum)
	positions, err := getSeekPositions(suite.factory, suite.pchannel, 100)
	assert.NoError(suite.T(), err)
	assert.NotEqual(suite.T(), 0, len(positions))

	for i := 0; i < vchannelNum; i++ {
		vchannel := fmt.Sprintf("%s_vchannelv%d", suite.pchannel, i)
		output, err := suite.manager.Add(context.Background(), NewStreamConfig(
			vchannel, positions[rand.Intn(len(positions))],
			common.SubscriptionPositionUnknown,
		)) // seek from random position
		require.NoError(suite.T(), err)
		suite.vchannels[vchannel] = &vchannelHelper{output: output}
	}
	wg := &sync.WaitGroup{}
	for vchannel := range suite.vchannels {
		wg.Add(1)
		go suite.consumeMsg(ctx, wg, vchannel)
	}

	suite.Eventually(func() bool {
		suite.T().Logf("dispatcherManager.dispatcherNum = %d", suite.manager.NumConsumer())
		return suite.manager.NumConsumer() == 1 // expected all merged, only mainDispatcher exist
	}, 15*time.Second, 100*time.Millisecond)
	assert.Equal(suite.T(), vchannelNum, suite.manager.NumTarget())

	cancel()
	wg.Wait()
}

func (suite *SimulationSuite) TestSplit() {
	ctx, cancel := context.WithCancel(context.Background())
	go suite.produceTimeTickOnly(ctx)

	const (
		vchannelNum = 10
		splitNum    = 3
	)
	suite.vchannels = make(map[string]*vchannelHelper, vchannelNum)
	maxTolerantLagK := paramtable.Get().MQCfg.MaxTolerantLag.Key
	paramtable.Get().Save(maxTolerantLagK, "0.5")
	defer paramtable.Get().Reset(maxTolerantLagK)

	targetBufSizeK := paramtable.Get().MQCfg.TargetBufSize.Key
	defer paramtable.Get().Reset(targetBufSizeK)

	for i := 0; i < vchannelNum; i++ {
		paramtable.Get().Save(targetBufSizeK, "65536")
		if i >= vchannelNum-splitNum {
			paramtable.Get().Save(targetBufSizeK, "10")
		}
		vchannel := fmt.Sprintf("%s_vchannelv%d", suite.pchannel, i)
		_, err := suite.manager.Add(context.Background(), NewStreamConfig(vchannel, nil, common.SubscriptionPositionEarliest))
		assert.NoError(suite.T(), err)
	}

	suite.Eventually(func() bool {
		suite.T().Logf("dispatcherManager.dispatcherNum = %d, splitNum+1 = %d", suite.manager.NumConsumer(), splitNum+1)
		return suite.manager.NumConsumer() == splitNum+1 // expected 1 mainDispatcher and `splitNum` soloDispatchers
	}, 10*time.Second, 100*time.Millisecond)
	assert.Equal(suite.T(), vchannelNum, suite.manager.NumTarget())

	cancel()
}

func (suite *SimulationSuite) TearDownTest() {
	for vchannel := range suite.vchannels {
		suite.manager.Remove(vchannel)
	}
	suite.manager.Close()
}

func (suite *SimulationSuite) TearDownSuite() {
}

func TestSimulation(t *testing.T) {
	suite.Run(t, new(SimulationSuite))
}
