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

package resource

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

func TestRecordingGuardRecordsWhatItWasAsked(t *testing.T) {
	g := NewRecordingGuard()
	req := taskresource.Requirement{CPU: 1, Memory: 2 << 30}

	require.NoError(t, g.Accept(context.Background(), 1, taskcommon.Compaction, req))
	g.Note("work")
	g.Release(1)

	assert.Equal(t, []AcquireCall{{TaskID: 1, Type: taskcommon.Compaction, Req: req}}, g.Acquires())
	assert.Equal(t, []int64{1}, g.Releases())
	// The ORDER is the point: an executor that ran work before accepting, or
	// released before finishing, would show up here and nowhere else.
	assert.Equal(t, []string{"acquire", "work", "release"}, g.Events())
	// Snapshot is inert: nothing under test reads the ledger through the double.
	assert.Equal(t, Snapshot{}, g.Snapshot())
}

func TestRecordingGuardFailedAcceptRecordsNothing(t *testing.T) {
	g := NewRecordingGuard()
	boom := errors.New("boom")
	g.FailAcquire(boom)

	err := g.Accept(context.Background(), 1, taskcommon.Import, taskresource.Requirement{})

	assert.ErrorIs(t, err, boom)
	assert.Empty(t, g.Acquires())
	assert.Empty(t, g.Events())
}

func TestRecordingGuardBlockedAcceptHonoursContext(t *testing.T) {
	g := NewRecordingGuard()
	g.Block()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := g.Accept(ctx, 1, taskcommon.Stats, taskresource.Requirement{})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, g.Acquires())
}
