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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
// its Stop, which runs on shutdown. The lock guards nothing but the flags: it
// is never held across Start or Stop, so a shutdown that arrives while Start is
// running returns at once and the engine's own Stop is what interrupts the
// Start (see extension.CoordinatorEngine). The flags decide three things:
//
//   - Stop before or without Start is a no-op. A standby is stopped without
//     ever having been activated, and its engine was never started; the seam
//     does not call Stop on it.
//   - Stop before Start also cancels the Start: an activation that fires after
//     shutdown began does not start an engine nothing will stop. "Before" here
//     means before Start was entered, not before it was claimed: a Start is
//     claimed on one goroutine and entered a few instructions later, and a Stop
//     landing in between waits for that goroutine to enter or cancel, so the
//     engine never sees a Stop followed by a Start.
//   - Start runs at most once, whatever fires it.
type engineLifecycle struct {
	mu      sync.Mutex
	started bool
	stopped bool
	entered bool
	// settled is closed by the goroutine that claimed the Start, once it has
	// either entered Start or given it up. It is nil until a Start is claimed.
	settled chan struct{}
}

var lifecycle engineLifecycle

// beginStart claims the one Start, and refuses it once Stop has been asked for.
// The caller owns the claim and must settle it with enterStart.
func (l *engineLifecycle) beginStart() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started || l.stopped {
		return false
	}
	l.started = true
	l.settled = make(chan struct{})
	return true
}

// enterStart settles a claim: it reports whether the engine's Start is to be
// entered now, which it is unless a Stop arrived in the meantime, and releases
// any Stop waiting on the answer.
func (l *engineLifecycle) enterStart() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.stopped {
		l.entered = true
	}
	close(l.settled)
	return l.entered
}

// beginStop claims the one Stop, and reports whether there is a started engine
// to stop. A claimed but not yet entered Start is waited for: the wait is the
// few instructions between the two, and it is what keeps the engine from being
// stopped before it is started.
func (l *engineLifecycle) beginStop() bool {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return false
	}
	l.stopped = true
	settled, started, entered := l.settled, l.started, l.entered
	l.mu.Unlock()

	if !started || entered {
		return entered
	}
	<-settled
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entered
}

// startCoordinatorEngine arranges for the installed engine to be started over
// the coordinator client, once this replica is ACTIVE and serving is the state
// it can call back into.
//
// The engine runs on a goroutine of its own, never on the activation callback
// chain: those callbacks run to completion before the coordinator's recovery
// barrier opens, and gRPC Serve waits on that barrier, so an engine started on
// that chain would block the very service its first call needs. waitServing is
// what it waits on instead. A start failure is fatal, through onFatal: a
// coordinator serving without its engine would accept work nothing accounts
// for, and by then the startup path it could have been returned to is gone.
func startCoordinatorEngine(
	ctx context.Context,
	coord types.MixCoordComponent,
	client types.MixCoordClient,
	waitServing func(context.Context) error,
	onFatal func(error),
) error {
	engine := coordinatorEngine()
	if engine == nil {
		return nil
	}
	notifier, ok := coord.(activeNotifier)
	if !ok {
		return merr.WrapErrServiceInternal(
			"coordinator engine installed, but the coordinator does not report activation")
	}
	start := func() error {
		if !lifecycle.beginStart() {
			mlog.Info(ctx, "coordinator engine not started: already started, or stopped before activation")
			return nil
		}
		if !lifecycle.enterStart() {
			mlog.Info(ctx, "coordinator engine not started: shutdown began before it could start")
			return nil
		}
		if err := engine.Start(ctx, client); err != nil {
			return err
		}
		mlog.Info(ctx, "coordinator engine started")
		return nil
	}
	notifier.OnActive(func() {
		go func() {
			if err := waitServing(ctx); err != nil {
				mlog.Info(ctx, "coordinator engine not started: the coordinator stopped before it served",
					mlog.Err(err))
				return
			}
			if err := start(); err != nil {
				onFatal(err)
			}
		}()
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
