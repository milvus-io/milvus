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

package extension

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDQLObserver struct {
	starts, finishes atomic.Int32
	mu               sync.Mutex
	phases           []string
}

func (o *testDQLObserver) BeginAttempt() func() { o.starts.Add(1); return func() { o.finishes.Add(1) } }
func (o *testDQLObserver) SetRequestPhase(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.phases = append(o.phases, s)
}

func TestDQLObserverNestedAttemptsAndRetries(t *testing.T) {
	root := context.Background()
	same, done := BeginDQLAttempt(root)
	require.Equal(t, root, same)
	done()
	require.Equal(t, root, WithDQLObserver(root, nil))
	observer := &testDQLObserver{}
	ctx := WithDQLObserver(root, observer)
	first, finish := BeginDQLAttempt(ctx)
	_, nested := BeginDQLAttempt(first)
	nested()
	require.EqualValues(t, 1, observer.starts.Load())
	require.Zero(t, observer.finishes.Load())
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Go(finish)
	}
	workers.Wait()
	require.EqualValues(t, 1, observer.finishes.Load())
	_, retry := BeginDQLAttempt(ctx)
	retry()
	require.EqualValues(t, 2, observer.starts.Load())
	require.EqualValues(t, 2, observer.finishes.Load())
	ObserveDQLPhase(root, "queue")
	ObserveDQLPhase(ctx, "compute")
	ObserveDQLPhase(ctx, "queue")
	ObserveDQLPhase(ctx, "execute")
	require.Equal(t, []string{"queue", "execute"}, observer.phases)
}

func TestDQLObserverIgnoresPhasesFromCompletedAttempts(t *testing.T) {
	observer := &testDQLObserver{}
	ctx := WithDQLObserver(context.Background(), observer)
	first, finish := BeginDQLAttempt(ctx)
	ObserveDQLPhase(first, "queue")
	finish()

	second, finishSecond := BeginDQLAttempt(ctx)
	ObserveDQLPhase(second, "queue")
	// A canceled task can leave its queue after its caller has returned.
	ObserveDQLPhase(first, "execute")
	require.Equal(t, []string{"queue", "queue"}, observer.phases)
	ObserveDQLPhase(second, "execute")
	finishSecond()
	ObserveDQLPhase(second, "queue")
	require.Equal(t, []string{"queue", "queue", "execute"}, observer.phases)
	require.EqualValues(t, 2, observer.finishes.Load())
}
