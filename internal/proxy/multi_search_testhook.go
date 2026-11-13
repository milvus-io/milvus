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
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// NewMultiSearchTestProxy starts the batch scheduler for tests using the public transports.
func NewMultiSearchTestProxy(t *testing.T, queueCapacity, workers int) *Proxy {
	t.Helper()
	s := newMultiSearchScheduler(t.Context(), queueCapacity)
	s.start(workers)
	t.Cleanup(s.stop)
	p := &Proxy{multiSearch: s}
	p.UpdateStateCode(commonpb.StateCode_Healthy)
	return p
}

type multiSearchTestTSO struct{ logical atomic.Int64 }

func (a *multiSearchTestTSO) AllocOne(context.Context) (taskmodel.Timestamp, error) {
	return tsoutil.ComposeTSByTimeWithLogical(time.Now(), a.logical.Add(1)), nil
}

// AddMultiSearchDQLSchedulerForTest attaches the ordinary DQL scheduler, with
// shard routing supplied by the test. The caller controls when it starts.
func AddMultiSearchDQLSchedulerForTest(t *testing.T, p *Proxy, cache Cache, policy shardclient.LBPolicy) *scheduler.TaskScheduler {
	t.Helper()
	sched, err := scheduler.NewTaskScheduler(t.Context(), &multiSearchTestTSO{})
	require.NoError(t, err)
	t.Cleanup(sched.Close)
	p.sched = sched
	p.setMetaCache(cache)
	p.lbPolicy = policy
	return sched
}
