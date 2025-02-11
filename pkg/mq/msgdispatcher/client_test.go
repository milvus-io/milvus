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

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	go produceTimeTick(ctx, producer)

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
	go produceTimeTick(ctx, producer)
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
	expected := int(total - deregisterCount.Load())
	expected = 1

	c := client1.(*client)
	n := c.managers.Len()
	assert.Equal(t, expected, n)
}
