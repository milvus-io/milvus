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

package segments

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type grepScanFunc func(context.Context, *array.Int64, native.ScanRequest) (*native.ScanResult, error)

func (f grepScanFunc) ScanGrep(ctx context.Context, offsets *array.Int64, req native.ScanRequest) (*native.ScanResult, error) {
	return f(ctx, offsets, req)
}

func TestQueryScanGrepReadGateRetry(t *testing.T) {
	pool := conc.NewPool[any](2)
	defer pool.Release()
	patch := mockey.Mock(GetSQPool).Return(pool).Build()
	defer patch.UnPatch()
	busy := merr.Wrap(merr.SegcoreError(2037, "segment read gate busy for segment 1"), "scan grep segment")
	unrelated := merr.SegcoreError(2037, "unrelated folly failure")
	wrongCode := merr.SegcoreError(2039, "segment read gate busy")
	for _, tc := range []struct {
		name        string
		segmentType SegmentType
		failure     error
		cancel      bool
		calls       int
	}{
		{"sealed busy recovers", SegmentTypeSealed, busy, false, 2},
		{"growing does not retry", SegmentTypeGrowing, busy, false, 1},
		{"unrelated error", SegmentTypeSealed, unrelated, false, 1},
		{"message without busy code", SegmentTypeSealed, wrongCode, false, 1},
		{"cancel while busy", SegmentTypeSealed, busy, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			builder := array.NewInt64Builder(alloc)
			builder.AppendValues([]int64{7, 11}, nil)
			offsets := builder.NewInt64Array()
			builder.Release()
			defer offsets.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := native.ScanRequest{FieldID: 100, Limit: 2}
			calls := 0
			var failed *native.ScanResult
			success := &native.ScanResult{Positions: []int{1}}
			segment := &queryScanGrepSegment{
				segmentType: tc.segmentType,
				Segment: grepScanFunc(func(actualCtx context.Context, actualOffsets *array.Int64, actualReq native.ScanRequest) (*native.ScanResult, error) {
					calls++
					assert.Same(t, ctx, actualCtx)
					assert.Same(t, offsets, actualOffsets)
					assert.Equal(t, req, actualReq)
					if calls == 1 {
						details := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
						details.Append([]byte("[]"))
						failed = &native.ScanResult{Details: details.NewArray()}
						details.Release()
						if tc.cancel {
							cancel()
						}
						return failed, tc.failure
					}
					assert.Nil(t, failed.Details, "failed result must be released before retry")
					return success, nil
				}),
			}
			result, err := segment.ScanGrep(ctx, offsets, req)
			defer result.Release()
			require.Equal(t, tc.calls, calls)
			require.Nil(t, failed.Details)
			if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			} else if tc.calls == 2 {
				require.NoError(t, err)
				require.Same(t, success, result)
			} else {
				require.ErrorIs(t, err, tc.failure)
				require.Nil(t, result)
			}
		})
	}
}

func TestQueryScanGrepSharedPool(t *testing.T) {
	for _, cancelQueued := range []bool{false, true} {
		name := "shared concurrency"
		if cancelQueued {
			name = "cancel queued request"
		}
		t.Run(name, func(t *testing.T) {
			pool := conc.NewPool[any](1)
			defer pool.Release()
			patch := mockey.Mock(GetSQPool).Return(pool).Build()
			defer patch.UnPatch()
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			var unblock sync.Once
			var workers sync.WaitGroup
			defer func() { unblock.Do(func() { close(release) }); workers.Wait() }()
			var calls atomic.Int32
			runner := grepScanFunc(func(context.Context, *array.Int64, native.ScanRequest) (*native.ScanResult, error) {
				calls.Add(1)
				entered <- struct{}{}
				<-release
				return &native.ScanResult{}, nil
			})
			first := &queryScanGrepSegment{Segment: runner, segmentType: SegmentTypeSealed}
			second := &queryScanGrepSegment{Segment: runner, segmentType: SegmentTypeSealed}
			done := make(chan error, 2)
			launch := func(ctx context.Context, segment *queryScanGrepSegment) {
				workers.Add(1)
				go func() {
					defer workers.Done()
					result, err := segment.ScanGrep(ctx, nil, native.ScanRequest{})
					result.Release()
					done <- err
				}()
			}
			launch(context.Background(), first)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first grep did not start")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			launch(ctx, second)
			require.Eventually(t, func() bool { return pool.Waiting() == 1 }, 5*time.Second, time.Millisecond)
			require.EqualValues(t, 1, calls.Load())
			if cancelQueued {
				cancel()
			}
			unblock.Do(func() { close(release) })
			var canceled int
			for i := 0; i < 2; i++ {
				select {
				case err := <-done:
					if err != nil {
						require.ErrorIs(t, err, context.Canceled)
						canceled++
					}
				case <-time.After(5 * time.Second):
					t.Fatal("grep did not finish")
				}
			}
			if cancelQueued {
				require.Equal(t, 1, canceled)
				require.EqualValues(t, 1, calls.Load())
			} else {
				require.Zero(t, canceled)
				require.EqualValues(t, 2, calls.Load())
			}
		})
	}
}

func TestQueryScanGrepBackoffReleasesPool(t *testing.T) {
	pool := conc.NewPool[any](1)
	defer pool.Release()
	patch := mockey.Mock(GetSQPool).Return(pool).Build()
	defer patch.UnPatch()
	waiting, resume := make(chan struct{}), make(chan struct{})
	waitPatch := mockey.Mock(waitSegmentReadGateRetry).To(func(context.Context, int) error {
		close(waiting)
		<-resume
		return nil
	}).Build()
	defer waitPatch.UnPatch()
	var unblock sync.Once
	var workers sync.WaitGroup
	defer func() { unblock.Do(func() { close(resume) }); workers.Wait() }()
	calls := 0
	segment := &queryScanGrepSegment{segmentType: SegmentTypeSealed, Segment: grepScanFunc(func(context.Context, *array.Int64, native.ScanRequest) (*native.ScanResult, error) {
		calls++
		if calls == 1 {
			return nil, merr.SegcoreError(2037, "segment read gate busy for segment 1")
		}
		return &native.ScanResult{}, nil
	})}
	done := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		result, err := segment.ScanGrep(context.Background(), nil, native.ScanRequest{})
		result.Release()
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("grep did not enter backoff")
	}
	available := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := pool.Submit(func() (any, error) { return nil, nil }).Await()
		available <- err
	}()
	select {
	case err := <-available:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("grep backoff occupied the pool worker")
	}
	unblock.Do(func() { close(resume) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("grep retry did not finish")
	}
	require.Equal(t, 2, calls)
}
