//go:build test

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

package proxy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/reqregistry"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// NewMultiSearchTestProxy starts the real task scheduler for transport tests.
func NewMultiSearchTestProxy(t *testing.T, queueCapacity, workers int) *Proxy {
	t.Helper()
	s, err := scheduler.NewTaskScheduler(t.Context(), &multiSearchTestTSO{}, scheduler.WithMultiSearchQueue(queueCapacity, workers))
	require.NoError(t, err)
	require.NoError(t, s.Start())
	t.Cleanup(s.Close)
	p := &Proxy{sched: s, requests: reqregistry.New()}
	p.setMetaCache(&multiSearchTestCache{})
	p.UpdateStateCode(commonpb.StateCode_Healthy)
	return p
}

// Batch task IDs use the cache allocator; ordinary DQL tasks still use TSO.
type multiSearchTestCache struct {
	Cache
	id atomic.Int64
}

func (c *multiSearchTestCache) AllocID(context.Context) (int64, error) {
	return c.id.Add(1), nil
}

type multiSearchTestTSO struct{ logical atomic.Int64 }

func (a *multiSearchTestTSO) AllocOne(context.Context) (taskmodel.Timestamp, error) {
	return tsoutil.ComposeTSByTimeWithLogical(time.Now(), a.logical.Add(1)), nil
}

// SetMultiSearchSearchDependenciesForTest supplies metadata and shard routing
// to ordinary Search on the same scheduler as the outer batch.
func SetMultiSearchSearchDependenciesForTest(p *Proxy, cache Cache, policy shardclient.LBPolicy) *scheduler.TaskScheduler {
	p.setMetaCache(&multiSearchTestCache{Cache: cache})
	p.lbPolicy = policy
	return p.sched
}
