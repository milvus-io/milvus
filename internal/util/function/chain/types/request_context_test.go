// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestRequestContextIsAValueSnapshotAndPreservesParentLifecycle(t *testing.T) {
	parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	original := RequestContext{Database: "db", Collection: "documents", User: "alice", ResourceName: "entry"}
	ctx := WithRequestContext(parent, original)
	original.User = "changed after bind"
	copy := GetRequestContext(ctx)
	copy.Collection = "changed after read"
	require.Equal(t, RequestContext{Database: "db", Collection: "documents", User: "alice", ResourceName: "entry"}, GetRequestContext(ctx))
	deadline, present := ctx.Deadline()
	parentDeadline, parentPresent := parent.Deadline()
	require.Equal(t, parentPresent, present)
	require.Equal(t, parentDeadline, deadline)
	require.Equal(t, parent.Done(), ctx.Done())
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Zero(t, GetRequestContext(context.Background()))
	require.Zero(t, GetRequestContext(nil))
}

func TestFuncContextScopeCloneRetainsExecutionSettings(t *testing.T) {
	parent := NewFuncContextFull(context.Background(), memory.DefaultAllocator, StagePostProcess).
		WithQueryContext(&QueryContext{}).WithTableResult(true).WithReduction(true).WithOutputRowLimit(16)
	request := WithRequestContext(parent.Context(), RequestContext{Database: "db", ResourceName: "summary/step/0/Aggregate"})
	child := parent.WithContext(request)
	require.Equal(t, "db", child.RequestContext().Database)
	require.Equal(t, "summary/step/0/Aggregate", child.RequestContext().ResourceName)
	require.Same(t, parent.QueryContext(), child.QueryContext())
	require.Same(t, parent.Pool(), child.Pool())
	require.Equal(t, parent.Stage(), child.Stage())
	require.True(t, child.TableResult())
	require.True(t, child.Reduction())
	require.EqualValues(t, 16, child.OutputRowLimit())
	require.Zero(t, parent.RequestContext(), "binding one step must not modify its parent")
}
