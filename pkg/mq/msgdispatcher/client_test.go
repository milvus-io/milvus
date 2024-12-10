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
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestClient(t *testing.T) {
	client := NewClient(newMockFactory(), typeutil.ProxyRole, 1)
	assert.NotNil(t, client)
	_, err := client.Register(context.Background(), NewStreamConfig("mock_vchannel_0", nil, common.SubscriptionPositionUnknown))
	assert.NoError(t, err)
	_, err = client.Register(context.Background(), NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
	assert.NoError(t, err)
	assert.NotPanics(t, func() {
		client.Deregister("mock_vchannel_0")
		client.Close()
	})

	t.Run("with timeout ctx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		defer cancel()
		<-time.After(2 * time.Millisecond)

		client := NewClient(newMockFactory(), typeutil.DataNodeRole, 1)
		defer client.Close()
		assert.NotNil(t, client)
		_, err := client.Register(ctx, NewStreamConfig("mock_vchannel_1", nil, common.SubscriptionPositionUnknown))
		assert.Error(t, err)
	})
}

func TestClient_Concurrency(t *testing.T) {
	client1 := NewClient(newMockFactory(), typeutil.ProxyRole, 1)
	assert.NotNil(t, client1)
	wg := &sync.WaitGroup{}
	const total = 100
	deregisterCount := atomic.NewInt32(0)
	for i := 0; i < total; i++ {
		vchannel := fmt.Sprintf("mock-vchannel-%d-%d", i, rand.Int())
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

	c := client1.(*client)
	n := c.managers.Len()
	assert.Equal(t, expected, n)
}
