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
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestManager(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().MQCfg.TargetBufSize.Key, "65536")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.TargetBufSize.Key)

	paramtable.Get().Save(paramtable.Get().MQCfg.MaxDispatcherNumPerPchannel.Key, "65536")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.MaxDispatcherNumPerPchannel.Key)

	t.Run("test add and remove dispatcher", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

		factory := newMockFactory()
		producer, err := newMockProducer(factory, pchannel)
		assert.NoError(t, err)
		go produceTimeTick(ctx, producer)

		c := NewDispatcherManager(pchannel, typeutil.ProxyRole, 1, factory)
		assert.NotNil(t, c)
		go c.Run()
		defer c.Close()
		assert.Equal(t, 0, c.NumConsumer())
		assert.Equal(t, 0, c.NumTarget())

		var offset int
		for i := 0; i < 30; i++ {
			r := rand.Intn(5) + 1
			for j := 0; j < r; j++ {
				offset++
				vchannel := fmt.Sprintf("%s_vchannelv%d", pchannel, offset)
				t.Logf("add vchannel, %s", vchannel)
				_, err := c.Add(ctx, NewStreamConfig(vchannel, nil, common.SubscriptionPositionUnknown))
				assert.NoError(t, err)
			}
			assert.Eventually(t, func() bool {
				t.Logf("offset=%d, numConsumer=%d, numTarget=%d", offset, c.NumConsumer(), c.NumTarget())
				return c.NumTarget() == offset
			}, 3*time.Second, 10*time.Millisecond)
			for j := 0; j < rand.Intn(r); j++ {
				vchannel := fmt.Sprintf("%s_vchannelv%d", pchannel, offset)
				t.Logf("remove vchannel, %s", vchannel)
				c.Remove(vchannel)
				offset--
			}
			assert.Eventually(t, func() bool {
				t.Logf("offset=%d, numConsumer=%d, numTarget=%d", offset, c.NumConsumer(), c.NumTarget())
				return c.NumTarget() == offset
			}, 3*time.Second, 10*time.Millisecond)
		}
	})

	t.Run("test merge and split", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().MQCfg.TargetBufSize.Key, "16")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

		factory := newMockFactory()
		producer, err := newMockProducer(factory, pchannel)
		assert.NoError(t, err)
		go produceTimeTick(ctx, producer)

		c := NewDispatcherManager(pchannel, typeutil.ProxyRole, 1, factory)
		assert.NotNil(t, c)

		go c.Run()
		defer c.Close()

		paramtable.Get().Save(paramtable.Get().MQCfg.MaxTolerantLag.Key, "0.5")
		defer paramtable.Get().Reset(paramtable.Get().MQCfg.MaxTolerantLag.Key)

		o0, err := c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-0", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		o1, err := c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-1", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		o2, err := c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-2", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		assert.Equal(t, 3, c.NumTarget())

		consumeFn := func(output <-chan *MsgPack, done <-chan struct{}, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case <-output:
				}
			}
		}
		wg := &sync.WaitGroup{}
		wg.Add(3)
		d0 := make(chan struct{}, 1)
		d1 := make(chan struct{}, 1)
		d2 := make(chan struct{}, 1)
		go consumeFn(o0, d0, wg)
		go consumeFn(o1, d1, wg)
		go consumeFn(o2, d2, wg)

		assert.Eventually(t, func() bool {
			return c.NumConsumer() == 1 // expected merge
		}, 20*time.Second, 10*time.Millisecond)

		// stop consume vchannel_2 to trigger split
		d2 <- struct{}{}
		assert.Eventually(t, func() bool {
			t.Logf("c.NumConsumer=%d", c.NumConsumer())
			return c.NumConsumer() == 2 // expected split
		}, 20*time.Second, 10*time.Millisecond)

		// stop all
		d0 <- struct{}{}
		d1 <- struct{}{}
		wg.Wait()
	})

	t.Run("test run and close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

		factory := newMockFactory()
		producer, err := newMockProducer(factory, pchannel)
		assert.NoError(t, err)
		go produceTimeTick(ctx, producer)

		c := NewDispatcherManager(pchannel, typeutil.ProxyRole, 1, factory)
		assert.NotNil(t, c)

		go c.Run()
		defer c.Close()

		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-0", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-1", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-2", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		assert.Equal(t, 3, c.NumTarget())
		assert.Eventually(t, func() bool {
			return c.NumConsumer() >= 1
		}, 3*time.Second, 10*time.Millisecond)
		c.(*dispatcherManager).mainDispatcher.curTs.Store(1000)
		for _, d := range c.(*dispatcherManager).deputyDispatchers {
			d.curTs.Store(1000)
		}

		checkIntervalK := paramtable.Get().MQCfg.MergeCheckInterval.Key
		paramtable.Get().Save(checkIntervalK, "0.01")
		defer paramtable.Get().Reset(checkIntervalK)

		assert.Eventually(t, func() bool {
			return c.NumConsumer() == 1 // expected merged
		}, 3*time.Second, 10*time.Millisecond)
		assert.Equal(t, 3, c.NumTarget())
	})

	t.Run("test_repeated_vchannel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

		factory := newMockFactory()
		producer, err := newMockProducer(factory, pchannel)
		assert.NoError(t, err)
		go produceTimeTick(ctx, producer)

		c := NewDispatcherManager(pchannel, typeutil.ProxyRole, 1, factory)

		go c.Run()
		defer c.Close()

		assert.NotNil(t, c)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-0", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-1", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-2", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.NoError(t, err)

		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-0", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-1", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
		_, err = c.Add(ctx, NewStreamConfig(fmt.Sprintf("%s_vchannel-2", pchannel), nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)

		assert.Eventually(t, func() bool {
			return c.NumConsumer() >= 1
		}, 3*time.Second, 10*time.Millisecond)
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

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

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
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.wg = &sync.WaitGroup{}

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
			if pack == nil || pack.EndTs == 0 {
				continue
			}
			assert.Greater(suite.T(), pack.EndTs, lastTs, fmt.Sprintf("vchannel=%s", vchannel))
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
	ctx, cancel := context.WithTimeout(suite.ctx, 5000*time.Millisecond)
	defer cancel()

	const (
		vchannelNum        = 10
		collectionID int64 = 1234
	)
	suite.vchannels = make(map[string]*vchannelHelper, vchannelNum)
	for i := 0; i < vchannelNum; i++ {
		vchannel := fmt.Sprintf("%s_%dv%d", suite.pchannel, collectionID, i)
		output, err := suite.manager.Add(ctx, NewStreamConfig(vchannel, nil, common.SubscriptionPositionEarliest))
		assert.NoError(suite.T(), err)
		suite.vchannels[vchannel] = &vchannelHelper{output: output}
	}

	wg := suite.wg
	wg.Add(1)
	go suite.produceMsg(wg, collectionID)
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
	go suite.produceTimeTickOnly(suite.ctx)

	const vchannelNum = 10
	suite.vchannels = make(map[string]*vchannelHelper, vchannelNum)
	positions, err := getSeekPositions(suite.factory, suite.pchannel, 100)
	assert.NoError(suite.T(), err)
	assert.NotEqual(suite.T(), 0, len(positions))

	for i := 0; i < vchannelNum; i++ {
		vchannel := fmt.Sprintf("%s_vchannelv%d", suite.pchannel, i)
		output, err := suite.manager.Add(suite.ctx, NewStreamConfig(
			vchannel, positions[rand.Intn(len(positions))],
			common.SubscriptionPositionUnknown,
		)) // seek from random position
		require.NoError(suite.T(), err)
		suite.vchannels[vchannel] = &vchannelHelper{output: output}
	}
	for vchannel := range suite.vchannels {
		suite.wg.Add(1)
		go suite.consumeMsg(suite.ctx, suite.wg, vchannel)
	}

	suite.Eventually(func() bool {
		suite.T().Logf("dispatcherManager.dispatcherNum = %d", suite.manager.NumConsumer())
		return suite.manager.NumConsumer() == 1 // expected all merged, only mainDispatcher exist
	}, 15*time.Second, 100*time.Millisecond)
	assert.Equal(suite.T(), vchannelNum, suite.manager.NumTarget())
}

func (suite *SimulationSuite) TestSplit() {
	go suite.produceTimeTickOnly(suite.ctx)

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
		paramtable.Get().Save(targetBufSizeK, "512")
		vchannel := fmt.Sprintf("%s_vchannelv%d", suite.pchannel, i)
		output, err := suite.manager.Add(suite.ctx, NewStreamConfig(vchannel, nil, common.SubscriptionPositionEarliest))
		assert.NoError(suite.T(), err)
		suite.vchannels[vchannel] = &vchannelHelper{output: output}
	}

	suite.Eventually(func() bool {
		return suite.manager.NumConsumer() == 1 // expected all merged, only mainDispatcher exist
	}, 15*time.Second, 100*time.Millisecond)
	assert.Equal(suite.T(), vchannelNum, suite.manager.NumTarget())

	// produce additional MsgPacks to trigger lag and split
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < splitNum; i++ {
		suite.wg.Add(1)
		vchannel := fmt.Sprintf("%s_vchannelv%d", suite.pchannel, i)
		target, ok := suite.manager.(*dispatcherManager).registeredTargets.Get(vchannel)
		assert.True(suite.T(), ok)
		go func() {
			defer suite.wg.Done()
			for {
				select {
				case target.ch <- &MsgPack{}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	suite.Eventually(func() bool {
		suite.T().Logf("dispatcherManager.dispatcherNum = %d, splitNum+1 = %d", suite.manager.NumConsumer(), splitNum+1)
		return suite.manager.NumConsumer() > 1 // expected 1 mainDispatcher and 1 or more split deputyDispatchers
	}, 20*time.Second, 100*time.Millisecond)
	assert.Equal(suite.T(), vchannelNum, suite.manager.NumTarget())

	cancel()
	suite.wg.Wait()

	for vchannel := range suite.vchannels {
		suite.wg.Add(1)
		go suite.consumeMsg(suite.ctx, suite.wg, vchannel)
	}
}

func (suite *SimulationSuite) TearDownTest() {
	for vchannel := range suite.vchannels {
		suite.manager.Remove(vchannel)
	}
	suite.manager.Close()
	suite.cancel()
	suite.wg.Wait()
}

func (suite *SimulationSuite) TearDownSuite() {
}

func TestSimulation(t *testing.T) {
	suite.Run(t, new(SimulationSuite))
}
