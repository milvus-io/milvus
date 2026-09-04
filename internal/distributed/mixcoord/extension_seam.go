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

package grpcmixcoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// The coordinator engine is control-plane machinery a deployment hosts in the
// coordinator process. With none installed the two functions below do nothing.

func coordinatorEngine() extension.CoordinatorEngine {
	return extension.InstalledCoordinatorEngine()
}

// activeNotifier is what the coordinator implements to run work once this
// replica is ACTIVE; a standby never fires it.
type activeNotifier interface {
	OnActive(fn func())
}

// engineLifecycle orders the engine's Start, which runs on activation, against
// its Stop, which runs on shutdown. The lock guards nothing but the two flags:
// it is never held across Start or Stop, so a shutdown that arrives during a
// slow Start returns at once and the engine's own Stop is what interrupts the
// Start (see extension.CoordinatorEngine). The flags decide three things:
//
//   - Stop before or without Start is a no-op. A standby is stopped without
//     ever having been activated, and its engine was never started; the seam
//     does not call Stop on it.
//   - Stop before Start also cancels the Start: an activation that fires after
//     shutdown began does not start an engine nothing will stop.
//   - Start runs at most once, whatever fires it.
type engineLifecycle struct {
	mu      sync.Mutex
	started bool
	stopped bool
}

var lifecycle engineLifecycle

// beginStart claims the one Start, and refuses it once Stop has been asked for.
func (l *engineLifecycle) beginStart() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started || l.stopped {
		return false
	}
	l.started = true
	return true
}

// beginStop claims the one Stop, and reports whether there is a started engine
// to stop.
func (l *engineLifecycle) beginStop() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return false
	}
	l.stopped = true
	return l.started
}

// startCoordinatorEngine starts the installed engine over the coordinator
// client once this replica is ACTIVE. A start failure on activation is fatal:
// a coordinator serving without its engine would accept work nothing accounts
// for.
func startCoordinatorEngine(ctx context.Context, coord types.MixCoordComponent, client types.MixCoordClient) error {
	engine := coordinatorEngine()
	if engine == nil {
		return nil
	}
	start := func() error {
		if !lifecycle.beginStart() {
			mlog.Info(ctx, "coordinator engine not started: already started, or stopped before activation")
			return nil
		}
		if err := engine.Start(ctx, client); err != nil {
			return err
		}
		mlog.Info(ctx, "coordinator engine started")
		return nil
	}
	notifier, ok := coord.(activeNotifier)
	if !ok {
		return start()
	}
	notifier.OnActive(func() {
		if err := start(); err != nil {
			mlog.Panic(ctx, "coordinator engine failed to start on activation", mlog.Err(err))
		}
	})
	return nil
}

// stopCoordinatorEngine stops the installed engine if it was started; its
// error is logged, not returned, because the coordinator shutdown must not hang
// on it. It does not wait for a Start still in progress: the engine's Stop is
// what ends that Start.
func stopCoordinatorEngine(ctx context.Context) {
	engine := coordinatorEngine()
	if engine == nil {
		return
	}
	if !lifecycle.beginStop() {
		mlog.Info(ctx, "coordinator engine not stopped: it was never started")
		return
	}
	if err := engine.Stop(); err != nil {
		mlog.Warn(ctx, "coordinator engine stop failed", mlog.Err(err))
	}
}
