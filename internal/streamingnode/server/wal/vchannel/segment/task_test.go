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

package segment

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSegmentTaskDelaysUntilPredecessorIsTerminal(t *testing.T) {
	predecessor := &testSegmentTask{err: errors.New("business failure")}
	successor := &testSegmentTask{
		segmentTaskBase: segmentTaskBase{predecessors: []segmentTask{predecessor}},
	}

	require.ErrorIs(t, successor.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.Equal(t, int32(0), successor.calls.Load())
	assert.False(t, successor.Done())

	require.Error(t, predecessor.Execute(context.Background()))
	assert.True(t, predecessor.Done())
	require.NoError(t, successor.Execute(context.Background()))
	assert.Equal(t, int32(1), successor.calls.Load())
	assert.True(t, successor.Done())
}

type testSegmentTask struct {
	segmentTaskBase
	err   error
	calls atomic.Int32
}

func (t *testSegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(context.Context) error {
		t.calls.Add(1)
		return t.err
	})
}
