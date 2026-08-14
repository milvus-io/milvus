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
	"sync"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// AcquireCall is one admission request seen by a RecordingGuard.
type AcquireCall struct {
	TaskID int64
	Type   taskcommon.Type
	Req    taskresource.Requirement
}

// RecordingGuard is a Guard double for the tests of the executors that consume
// the guard. It lives here rather than in each executor's test package because
// all three executors need the same three things from it: what was asked for,
// in what order relative to the work, and whether the caller parks in Acquire
// or polls TryAcquire -- a distinction the real guard cannot expose across a
// package boundary, and one that decides whether a large task can be starved.
type RecordingGuard struct {
	mu sync.Mutex

	acquires    []AcquireCall
	tryAcquires []AcquireCall
	releases    []int64
	events      []string

	// gate, when non-nil, holds every Acquire open until it is closed.
	gate chan struct{}
	// err, when non-nil, is what Acquire returns instead of reserving.
	err error
	// snapshot is what Snapshot reports. The zero value is deliberately the
	// zero Snapshot, which is what every existing caller of this double already
	// gets; SetSnapshot is for the tests that need the reporting path to see a
	// node under load, since a zero Total makes legacyAvailableSlots
	// short-circuit and report the node completely free -- which is also what
	// the code this branch replaces would have reported, so a fixture that
	// leaves it zero cannot tell the old behavior from the new.
	snapshot Snapshot
}

var _ Guard = (*RecordingGuard)(nil)

func NewRecordingGuard() *RecordingGuard {
	return &RecordingGuard{}
}

// Block holds every later Acquire open until Unblock, so a test can observe
// what a caller does while it is queued.
func (g *RecordingGuard) Block() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gate == nil {
		g.gate = make(chan struct{})
	}
}

func (g *RecordingGuard) Unblock() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
}

// FailAcquire makes Acquire return err without reserving, standing in for the
// one failure the real guard has: the caller's context ending.
func (g *RecordingGuard) FailAcquire(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.err = err
}

// Note appends a marker to the same ordered log the guard calls land in, so a
// test can assert where the work sits relative to the reservation.
func (g *RecordingGuard) Note(event string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, event)
}

func (g *RecordingGuard) Acquire(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error {
	g.mu.Lock()
	gate, err := g.gate, g.err
	g.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.acquires = append(g.acquires, AcquireCall{TaskID: taskID, Type: taskType, Req: req})
	g.events = append(g.events, "acquire")
	return nil
}

func (g *RecordingGuard) TryAcquire(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tryAcquires = append(g.tryAcquires, AcquireCall{TaskID: taskID, Type: taskType, Req: req})
	g.events = append(g.events, "tryAcquire")
	return true, taskresource.Capacity{}
}

func (g *RecordingGuard) Release(taskID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.releases = append(g.releases, taskID)
	g.events = append(g.events, "release")
}

// SetSnapshot fixes what Snapshot reports from here on.
func (g *RecordingGuard) SetSnapshot(s Snapshot) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.snapshot = s
}

func (g *RecordingGuard) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshot
}

func (g *RecordingGuard) Acquires() []AcquireCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]AcquireCall(nil), g.acquires...)
}

func (g *RecordingGuard) TryAcquires() []AcquireCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]AcquireCall(nil), g.tryAcquires...)
}

func (g *RecordingGuard) Releases() []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int64(nil), g.releases...)
}

// Events is the ordered log of guard calls and of whatever the caller recorded
// with Note.
func (g *RecordingGuard) Events() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.events...)
}
