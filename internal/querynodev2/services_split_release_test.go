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

package querynodev2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Releasing a source tears down its un-adopted children, and an un-adopted child
// that was itself split fronts un-adopted children of its own: those go too,
// instead of being left registered with running pipelines and no parent.
func TestReleaseSplitChildrenReleasesUnadoptedGrandchildren(t *testing.T) {
	paramtable.Init()
	manager := segments.NewManager()
	node := &QueryNode{
		ctx:        context.Background(),
		delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		manager:    manager,
	}
	node.pipelineManager = pipeline.NewManager(manager, nil, node.delegators)

	source := delegator.NewMockShardDelegator(t)
	source.EXPECT().MarkReleasing().Once()
	source.EXPECT().SplitChildVChannels().Return([]string{"v1"})

	child := delegator.NewMockShardDelegator(t)
	child.EXPECT().IsUnadoptedSplitChild().Return(true)
	child.EXPECT().MarkReleasing().Once()
	child.EXPECT().SplitChildVChannels().Return([]string{"v3"})
	child.EXPECT().Close().Once()

	grandchild := delegator.NewMockShardDelegator(t)
	grandchild.EXPECT().IsUnadoptedSplitChild().Return(true)
	grandchild.EXPECT().MarkReleasing().Once()
	grandchild.EXPECT().SplitChildVChannels().Return(nil)
	grandchild.EXPECT().Close().Once()

	node.delegators.Insert("v1", child)
	node.delegators.Insert("v3", grandchild)

	node.releaseSplitChildren(context.Background(), source, 1)

	assert.False(t, node.delegators.Contain("v1"))
	assert.False(t, node.delegators.Contain("v3"), "an un-adopted grandchild must not outlive its released parent")
}
