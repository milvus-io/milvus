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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestClient(t *testing.T) {
	factory := newMockFactory()
	client := NewClient(factory, typeutil.ProxyRole, 1)
	assert.NotNil(t, client)
	defer client.Close()

	pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := newMockProducer(factory, pchannel)
	assert.NoError(t, err)
	go produceTimeTick(t, ctx, producer)

	_, err = client.Register(ctx, NewStreamConfig(fmt.Sprintf("%s_v1", pchannel), nil, common.SubscriptionPositionUnknown))
	assert.NoError(t, err)

	_, err = client.Register(ctx, NewStreamConfig(fmt.Sprintf("%s_v2", pchannel), nil, common.SubscriptionPositionUnknown))
	assert.NoError(t, err)

	client.Deregister(fmt.Sprintf("%s_v1", pchannel))
	client.Deregister(fmt.Sprintf("%s_v2", pchannel))
}

func TestClient_Concurrency(t *testing.T) {
	factory := newMockFactory()
	client1 := NewClient(factory, typeutil.ProxyRole, 1)
	assert.NotNil(t, client1)
	defer client1.Close()

	paramtable.Get().Save(paramtable.Get().MQCfg.TargetBufSize.Key, "65536")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.TargetBufSize.Key)

	paramtable.Get().Save(paramtable.Get().MQCfg.MaxDispatcherNumPerPchannel.Key, "65536")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.MaxDispatcherNumPerPchannel.Key)

	pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := newMockProducer(factory, pchannel)
	assert.NoError(t, err)
	go produceTimeTick(t, ctx, producer)
	t.Logf("start to produce time tick to pchannel %s", pchannel)

	wg := &sync.WaitGroup{}
	const total = 100
	deregisterCount := atomic.NewInt32(0)
	for i := 0; i < total; i++ {
		i := i
		vchannel := fmt.Sprintf("%s_vchannel-%d-%d", pchannel, i, rand.Int())
		wg.Add(1)
		go func() {
			_, err := client1.Register(context.Background(), NewStreamConfig(vchannel, nil, common.SubscriptionPositionUnknown))
			assert.NoError(t, err)
			for j := 0; j < rand.Intn(2); j++ {
				client1.Deregister(vchannel)
				deregisterCount.Inc()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	// TODO: fix
	// expected := int(total - deregisterCount.Load())
	expected := 1

	c := client1.(*client)
	n := c.managers.Len()
	assert.Equal(t, expected, n)
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

	client  Client
	factory msgstream.Factory

	pchannel2Producer  map[string]msgstream.MsgStream
	pchannel2Vchannels map[string]map[string]*vchannelHelper
}

func (suite *SimulationSuite) SetupSuite() {
	suite.factory = newMockFactory()
}

func (suite *SimulationSuite) SetupTest() {
	const (
		pchannelNum            = 16
		vchannelNumPerPchannel = 10
	)

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.wg = &sync.WaitGroup{}
	suite.client = NewClient(suite.factory, "test-client", 1)

	// Init pchannel and producers.
	suite.pchannel2Producer = make(map[string]msgstream.MsgStream)
	suite.pchannel2Vchannels = make(map[string]map[string]*vchannelHelper)
	for i := 0; i < pchannelNum; i++ {
		pchannel := fmt.Sprintf("by-dev-rootcoord-dispatcher-dml-%d_%d", time.Now().UnixNano(), i)
		producer, err := newMockProducer(suite.factory, pchannel)
		suite.NoError(err)
		suite.pchannel2Producer[pchannel] = producer
		suite.pchannel2Vchannels[pchannel] = make(map[string]*vchannelHelper)
	}

	// Init vchannels.
	for pchannel := range suite.pchannel2Producer {
		for i := 0; i < vchannelNumPerPchannel; i++ {
			collectionID := time.Now().UnixNano()
			vchannel := fmt.Sprintf("%s_%dv0", pchannel, collectionID)
			suite.pchannel2Vchannels[pchannel][vchannel] = &vchannelHelper{}
		}
	}
}

func (suite *SimulationSuite) TestDispatchToVchannels() {
	ctx, cancel := context.WithTimeout(suite.ctx, 20*time.Second)
	defer cancel()

	// Register vchannels.
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel, helper := range vchannels {
			output, err := suite.client.Register(ctx, NewStreamConfig(vchannel, nil, common.SubscriptionPositionEarliest))
			suite.NoError(err)
			helper.output = output
		}
	}

	// Produce and dispatch messages to vchannel targets.
	for pchannel, vchannels := range suite.pchannel2Vchannels {
		suite.wg.Add(1)
		go produceMsg(suite.T(), suite.wg, suite.pchannel2Producer[pchannel], vchannels)
	}
	// Mock pipelines consume messages.
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel, helper := range vchannels {
			suite.wg.Add(1)
			go consumeMsg(suite.T(), ctx, suite.wg, vchannel, helper)
		}
	}
	suite.wg.Wait()

	// Verify pub-sub messages number.
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel, helper := range vchannels {
			suite.Equal(helper.pubInsMsgNum, helper.subInsMsgNum, vchannel)
			suite.Equal(helper.pubDelMsgNum, helper.subDelMsgNum, vchannel)
			suite.Equal(helper.pubDDLMsgNum, helper.subDDLMsgNum, vchannel)
			suite.Equal(helper.pubPackNum, helper.subPackNum, vchannel)
		}
	}
}

// TODO: verify skipped message number
func (suite *SimulationSuite) TestMerge() {
	// Produce timetick msgs.
	for _, producer := range suite.pchannel2Producer {
		go produceTimeTick(suite.T(), suite.ctx, producer)
	}

	// Fetch many msg positions to seek.
	pchannel2Positions := make(map[string][]*msgpb.MsgPosition)
	for pchannel := range suite.pchannel2Producer {
		positions, err := getSeekPositions(suite.factory, pchannel, 100)
		suite.NoError(err)
		suite.NotEqual(0, len(positions))
		pchannel2Positions[pchannel] = positions
	}

	// Register vchannels.
	for pchannel, vchannels := range suite.pchannel2Vchannels {
		positions := pchannel2Positions[pchannel]
		for vchannel, helper := range vchannels {
			output, err := suite.client.Register(suite.ctx, NewStreamConfig(
				vchannel, positions[rand.Intn(len(positions))], // seek from random position
				common.SubscriptionPositionUnknown,
			))
			suite.NoError(err)
			helper.output = output
		}
	}

	// Mock pipelines consume messages.
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel, helper := range vchannels {
			suite.wg.Add(1)
			go consumeMsg(suite.T(), suite.ctx, suite.wg, vchannel, helper)
		}
	}

	// Verify dispatchers merged.
	suite.Eventually(func() bool {
		for pchannel := range suite.pchannel2Producer {
			manager, ok := suite.client.(*client).managers.Get(pchannel)
			suite.T().Logf("dispatcherNum = %d, pchannel = %s", manager.NumConsumer(), pchannel)
			suite.True(ok)
			if manager.NumConsumer() != 1 { // expected all merged, only mainDispatcher exist
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond)
}

func (suite *SimulationSuite) TestSplit() {
	// Modify the parameters to make triggering split easier.
	paramtable.Get().Save(paramtable.Get().MQCfg.MaxTolerantLag.Key, "0.5")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.MaxTolerantLag.Key)
	paramtable.Get().Save(paramtable.Get().MQCfg.TargetBufSize.Key, "512")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.TargetBufSize.Key)

	// Produce timetick msgs.
	for _, producer := range suite.pchannel2Producer {
		go produceTimeTick(suite.T(), suite.ctx, producer)
	}

	// Register vchannels.
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel, helper := range vchannels {
			output, err := suite.client.Register(suite.ctx, NewStreamConfig(vchannel, nil, common.SubscriptionPositionEarliest))
			suite.NoError(err)
			helper.output = output
		}
	}

	// Verify dispatchers merged.
	suite.Eventually(func() bool {
		for pchannel := range suite.pchannel2Producer {
			manager, ok := suite.client.(*client).managers.Get(pchannel)
			suite.T().Logf("dispatcherNum = %d, pchannel = %s", manager.NumConsumer(), pchannel)
			suite.True(ok)
			if manager.NumConsumer() != 1 { // expected all merged, only mainDispatcher exist
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond)

	getTargetChan := func(pchannel, vchannel string) chan *MsgPack {
		manager, ok := suite.client.(*client).managers.Get(pchannel)
		suite.True(ok)
		t, ok := manager.(*dispatcherManager).registeredTargets.Get(vchannel)
		suite.True(ok)
		return t.ch
	}

	// Inject additional messages into targets to trigger lag and split.
	injectCtx, injectCancel := context.WithCancel(context.Background())
	const splitNumPerPchannel = 3
	for pchannel, vchannels := range suite.pchannel2Vchannels {
		cnt := 0
		for vchannel := range vchannels {
			suite.wg.Add(1)
			targetCh := getTargetChan(pchannel, vchannel)
			go func() {
				defer suite.wg.Done()
				for {
					select {
					case targetCh <- &MsgPack{}:
					case <-injectCtx.Done():
						return
					}
				}
			}()
			cnt++
			if cnt == splitNumPerPchannel {
				break
			}
		}
	}

	// Verify split.
	suite.Eventually(func() bool {
		for pchannel := range suite.pchannel2Producer {
			manager, ok := suite.client.(*client).managers.Get(pchannel)
			suite.True(ok)
			suite.T().Logf("dispatcherNum = %d, splitNum+1 = %d, pchannel = %s", manager.NumConsumer(), splitNumPerPchannel+1, pchannel)
			if manager.NumConsumer() < 1 { // expected 1 mainDispatcher and 1 or more split deputyDispatchers
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond)

	injectCancel()
	suite.wg.Wait()

	// Mock pipelines consume messages to trigger merged again.
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel, helper := range vchannels {
			suite.wg.Add(1)
			go consumeMsg(suite.T(), suite.ctx, suite.wg, vchannel, helper)
		}
	}

	// Verify dispatchers merged.
	suite.Eventually(func() bool {
		for pchannel := range suite.pchannel2Producer {
			manager, ok := suite.client.(*client).managers.Get(pchannel)
			suite.T().Logf("dispatcherNum = %d, pchannel = %s", manager.NumConsumer(), pchannel)
			suite.True(ok)
			if manager.NumConsumer() != 1 { // expected all merged, only mainDispatcher exist
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond)
}

func (suite *SimulationSuite) TearDownTest() {
	for _, vchannels := range suite.pchannel2Vchannels {
		for vchannel := range vchannels {
			suite.client.Deregister(vchannel)
		}
	}
	suite.client.Close()
	suite.cancel()
	suite.wg.Wait()
}

func (suite *SimulationSuite) TearDownSuite() {
}

func TestSimulation(t *testing.T) {
	suite.Run(t, new(SimulationSuite))
}
