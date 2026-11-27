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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// scopingHook is a request hook that pins every request to one resource group
// from Before, the way a deployment that routes queries per resource group
// does.
type scopingHook struct {
	hookutil.DefaultHook
	resourceGroup string
}

func (h scopingHook) Before(ctx context.Context, _ interface{}, _ string) (context.Context, error) {
	return extension.WithQueryResourceGroup(ctx, h.resourceGroup), nil
}

// The context a hook's Before returns is the context the handler runs under,
// so a resource group the hook pins reaches the search task - and from there
// the shard-leader routing - with no plumbing of its own.
func TestSearchRunsScopedToTheResourceGroupTheHookNamed(t *testing.T) {
	hookutil.InitOnceHook()
	hookutil.SetTestHook(scopingHook{resourceGroup: "rg-a"})
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	var scope string
	seen := false
	defer mockey.Mock((*scheduler.BaseTaskQueue).Enqueue).To(func(_ *scheduler.BaseTaskQueue, t taskmodel.Task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		seen = true
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*taskmodel.TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &scheduler.TaskScheduler{DqQueue: &scheduler.DqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	_, err := HookInterceptor(context.Background(),
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}, "user",
		milvuspb.MilvusService_Search_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return node.Search(ctx, req.(*milvuspb.SearchRequest))
		})
	require.NoError(t, err)
	require.True(t, seen, "the search must have reached the task queue")
	assert.Equal(t, "rg-a", scope,
		"the task must run scoped to the resource group the hook named, or routing ignores it")
}

func TestSearchTaskCarriesNoScopeWithTheDefaultHook(t *testing.T) {
	hookutil.InitOnceHook()
	hookutil.SetTestHook(hookutil.DefaultHook{})

	scope := "unset"
	defer mockey.Mock((*scheduler.BaseTaskQueue).Enqueue).To(func(_ *scheduler.BaseTaskQueue, t taskmodel.Task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*taskmodel.TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &scheduler.TaskScheduler{DqQueue: &scheduler.DqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	_, err := HookInterceptor(context.Background(),
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}, "user",
		milvuspb.MilvusService_Search_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return node.Search(ctx, req.(*milvuspb.SearchRequest))
		})
	require.NoError(t, err)
	assert.Equal(t, "", scope, "a stock binary must route across every replica of the collection")
}
