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

package delegator

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"

	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
)

func TestExecuteSubTasksPreservesWorkerStatusError(t *testing.T) {
	worker := cluster.NewMockWorker(t)
	tasks := []subTask[*querypb.QueryRequest]{
		{
			req:      &querypb.QueryRequest{},
			targetID: 1,
			worker:   worker,
		},
	}

	_, err := executeSubTasks(
		context.Background(),
		tasks,
		nil,
		func(context.Context, *querypb.QueryRequest, cluster.Worker) (*internalpb.RetrieveResults, error) {
			return &internalpb.RetrieveResults{
				Status: merr.Status(merr.WrapErrTooManyRequests(4, "limit by queryNode.scheduler.unsolvedQueueSize")),
			}, nil
		},
		"Query",
		log.Ctx(context.Background()),
	)

	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrServiceTooManyRequests), "expected worker status error to preserve service-too-many-requests cause")
}

func TestExecuteSubTasksUsesFirstWorkerErrorAsStatusCode(t *testing.T) {
	paramtable.Init()
	old := paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.SwapTempValue("0")
	t.Cleanup(func() {
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.SwapTempValue(old)
	})

	tasks := []subTask[int]{
		{req: 1, targetID: 1, worker: cluster.NewMockWorker(t)},
		{req: 2, targetID: 2, worker: cluster.NewMockWorker(t)},
	}

	_, err := executeSubTasks(
		context.Background(),
		tasks,
		nil,
		func(_ context.Context, req int, _ cluster.Worker) (*internalpb.RetrieveResults, error) {
			if req == 1 {
				return &internalpb.RetrieveResults{
					Status: merr.Status(errors.Wrap(merr.ErrServiceResourceInsufficient, "resource exhausted")),
				}, nil
			}
			return &internalpb.RetrieveResults{
				Status: merr.Status(merr.WrapErrTooManyRequests(4, "limit by queryNode.scheduler.unsolvedQueueSize")),
			}, nil
		},
		"Search",
		log.Ctx(context.Background()),
	)

	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
	require.NotErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Contains(t, err.Error(), "worker(2) query failed")

	roundTripErr := merr.Error(merr.Status(err))
	require.ErrorIs(t, roundTripErr, merr.ErrServiceResourceInsufficient)
	require.NotErrorIs(t, roundTripErr, merr.ErrServiceTooManyRequests)
}
