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

//go:build test

package packed

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func newTestTalonClient(t *testing.T) *TalonClient {
	t.Helper()
	c, err := NewTalonClient(&indexpb.StorageConfig{TalonCoordinator: "127.0.0.1:1"}, 1)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestTalonCancelledAdmission(t *testing.T) {
	c := newTestTalonClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := c.Stat(ctx, "s3://bucket/manifest")
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.Read(ctx, TalonReadRequest{URI: "s3://bucket/manifest", Length: 8})
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.Load(ctx, TalonLoadRequest{URI: "s3://bucket/manifest"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.BatchLoad(ctx, []TalonLoadRequest{{URI: "s3://bucket/manifest"}})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, c.io.native, "canceled admission must not initialize or dispatch native work")
}

func TestTalonEmptyBatch(t *testing.T) {
	c := newTestTalonClient(t)
	results, err := c.BatchLoad(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, results)
	require.Nil(t, c.io.native)
}

func TestTalonPartialLoadPayload(t *testing.T) {
	result := testTalonPartialLoadResult()
	var partial *TalonBatchLoadError
	require.ErrorAs(t, result.err, &partial)
	require.Equal(t, []TalonLoadFailure{
		{Index: 2, Uncertain: false, Diagnostic: "object not found"},
		{Index: 7, Uncertain: true, Diagnostic: "worker timeout: completion unknown"},
	}, partial.Failures)
	require.ErrorIs(t, partial, ErrLoonTransient)
	require.Contains(t, partial.Error(), "batch failed")
	require.Nil(t, result.loads, "native partial failures do not identify successful results")
}

func TestTalonStatPayload(t *testing.T) {
	result := testTalonStatResult()
	require.NoError(t, result.err)
	require.Equal(t, uint64(12345), result.size)
	require.Equal(t, "etag-123", result.version)
}

func TestTalonReadPayload(t *testing.T) {
	result := testTalonReadResult()
	require.NoError(t, result.err)
	require.Equal(t, int64(7), result.bytesRead)
}

func TestTalonLoadPayload(t *testing.T) {
	result := testTalonLoadResult(false)
	require.NoError(t, result.err)
	require.Equal(t, []TalonLoadResult{{Size: 17, Blocks: 1}}, result.loads)
}

func TestTalonBatchLoadPayload(t *testing.T) {
	result := testTalonLoadResult(true)
	require.NoError(t, result.err)
	require.Equal(t, []TalonLoadResult{{Size: 17, Blocks: 1}, {Size: 0, Blocks: 0}}, result.loads)
}

func TestTalonEmptyObjectOperations(t *testing.T) {
	c := newTestTalonClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	zero := uint64(0)
	// The configured coordinator is unreachable; known-empty requests must not
	// fetch metadata or data, and must retain one result per batch input.
	data, err := c.Read(ctx, TalonReadRequest{URI: "s3://bucket/empty", Length: 16, ObjectSize: &zero})
	require.NoError(t, err)
	require.Empty(t, data)
	result, err := c.Load(ctx, TalonLoadRequest{URI: "s3://bucket/empty"})
	require.NoError(t, err)
	require.Equal(t, TalonLoadResult{}, result)
	results, err := c.BatchLoad(ctx, []TalonLoadRequest{{URI: "s3://bucket/empty"}, {URI: "s3://bucket/other-empty"}})
	require.NoError(t, err)
	require.Equal(t, []TalonLoadResult{{}, {}}, results)
}

func TestTalonReadRejectsInvalidArguments(t *testing.T) {
	c := newTestTalonClient(t)
	_, err := c.Read(context.Background(), TalonReadRequest{URI: "s3://bucket/object", Length: ^uint64(0)})
	require.ErrorIs(t, err, ErrLoonPermanent)
	require.Nil(t, c.io.native, "unrepresentable lengths must fail before allocation or dispatch")
	_, err = c.Read(context.Background(), TalonReadRequest{URI: "s3://bucket/object", Length: 8, Version: "etag"})
	require.ErrorIs(t, err, ErrLoonPermanent, "a version without its object size is invalid")
	require.Empty(t, c.io.slots)
}

func TestTalonCancellationRetainsCallbackAdmission(t *testing.T) {
	for _, operation := range []string{"stat", "read", "load", "batch"} {
		t.Run(operation, func(t *testing.T) {
			c := newTestTalonClient(t)
			release := make(chan struct{})
			entered, err := testQueueManifestBlock(c.io, release)
			require.NoError(t, err)
			<-entered
			// Ensure a failing assertion cannot leave the executor blocked.
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "stat":
					_, _, err = c.Stat(ctx, "s3://bucket/manifest")
				case "read":
					_, err = c.Read(ctx, TalonReadRequest{URI: "s3://bucket/manifest", Length: 8})
				case "load":
					_, err = c.Load(ctx, TalonLoadRequest{URI: "s3://bucket/manifest", Size: 123, Version: "etag"})
				case "batch":
					_, err = c.BatchLoad(ctx, []TalonLoadRequest{{URI: "s3://bucket/manifest", Size: 123, Version: "etag"}})
				}
				done <- err
			}()
			// Admission alone precedes native submission. Wait for the blocked
			// worker plus the queued native task before canceling the waiter.
			require.Eventually(t, func() bool {
				c.io.taskMu.Lock()
				defer c.io.taskMu.Unlock()
				return c.io.pendingTasks == 2
			}, time.Second, time.Millisecond)
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("canceled Go waiter did not return")
			}
			require.Len(t, c.io.slots, 1, "accepted work owns admission until callback")
			closed := make(chan struct{})
			go func() { c.Close(); close(closed) }()
			select {
			case <-closed:
				t.Fatal("Close returned while native callback was queued")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not drain callback")
			}
			require.Empty(t, c.io.slots)
			_, _, err = c.Stat(context.Background(), "s3://bucket/manifest")
			require.Error(t, err)
		})
	}
}

func TestTalonRejectedSubmissionReleasesAdmission(t *testing.T) {
	c := newTestTalonClient(t)
	_, _, err := c.Stat(context.Background(), "")
	require.Error(t, err)
	require.Empty(t, c.io.slots)
	_, err = c.Read(context.Background(), TalonReadRequest{Length: 8})
	require.Error(t, err)
	require.Empty(t, c.io.slots)
	_, err = c.Load(context.Background(), TalonLoadRequest{URI: ""})
	require.Error(t, err)
	require.Empty(t, c.io.slots)
	_, err = c.BatchLoad(context.Background(), []TalonLoadRequest{{URI: ""}})
	require.Error(t, err)
	require.Empty(t, c.io.slots)
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("rejected operation retained native admission")
	}
}

func TestTalonConstructorRejectsInvalidConfig(t *testing.T) {
	for _, config := range []*indexpb.StorageConfig{nil, {}} {
		_, err := NewTalonClient(config, 1)
		require.Error(t, err)
	}
	_, err := NewTalonClient(&indexpb.StorageConfig{TalonCoordinator: "127.0.0.1:1"}, 0)
	require.Error(t, err)
}
