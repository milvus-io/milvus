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
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestClient(t *testing.T) {
	client := NewClient(newMockFactory(), typeutil.ProxyRole, 1)
	assert.NotNil(t, client)
	_, err := client.Register("mock_vchannel_0", nil, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	assert.NotPanics(t, func() {
		client.Deregister("mock_vchannel_0")
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
			_, err := client1.Register(vchannel, nil, mqwrapper.SubscriptionPositionUnknown)
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

	var n int
	client1.(*client).managers.Range(func(_, _ any) bool {
		n++
		return true
	})
	assert.Equal(t, expected, n)
}
