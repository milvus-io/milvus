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

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestManager(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().MQCfg.TargetBufSize.Key, "65536")
	defer paramtable.Get().Reset(paramtable.Get().MQCfg.TargetBufSize.Key)

	t.Run("test add and remove dispatcher", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pchannel := fmt.Sprintf("by-dev-rootcoord-dml_%d", rand.Int63())

		factory := newMockFactory()
		producer, err := newMockProducer(factory, pchannel)
		assert.NoError(t, err)
		go produceTimeTick(t, ctx, producer)

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
		go produceTimeTick(t, ctx, producer)

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
		go produceTimeTick(t, ctx, producer)

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
		}, 10*time.Second, 10*time.Millisecond)
		if c.(*dispatcherManager).mainDispatcher == nil {
			t.FailNow()
		}
		c.(*dispatcherManager).mainDispatcher.curTs.Store(1000)
		for _, d := range c.(*dispatcherManager).deputyDispatchers {
			d.curTs.Store(1000)
		}

		checkIntervalK := paramtable.Get().MQCfg.MergeCheckInterval.Key
		paramtable.Get().Save(checkIntervalK, "0.01")
		defer paramtable.Get().Reset(checkIntervalK)

		assert.Eventually(t, func() bool {
			return c.(*dispatcherManager).NumConsumer() == 1 // expected merged
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
		go produceTimeTick(t, ctx, producer)

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
