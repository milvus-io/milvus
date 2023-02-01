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

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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
	client := NewClient(newMockFactory(), typeutil.ProxyRole, 1)
	assert.NotNil(t, client)
	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		vchannel := fmt.Sprintf("mock-vchannel-%d-%d", i, rand.Int())
		wg.Add(1)
		go func() {
			for j := 0; j < 10; j++ {
				_, err := client.Register(vchannel, nil, mqwrapper.SubscriptionPositionUnknown)
				assert.NoError(t, err)
				client.Deregister(vchannel)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
