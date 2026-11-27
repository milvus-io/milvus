// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"context"
	"time"
)

// QueueBudget belongs to one original request, even after its task is merged.
// Zero MaxWait disables overload rejection, but not context/deadline expiry.
type QueueBudget struct {
	EnqueueTime time.Time
	MaxWait     time.Duration
}

func (b QueueBudget) ExpiryError(ctx context.Context, now time.Time) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deadline, ok := ctx.Deadline(); ok && !now.Before(deadline) {
			return context.DeadlineExceeded
		}
	}
	if b.MaxWait > 0 && now.Sub(b.EnqueueTime) > b.MaxWait {
		return queueOverloadedError(b.MaxWait, now.Sub(b.EnqueueTime))
	}
	return nil
}

// QueueBudgetTask retains each merged member's budget and removes only members
// that cannot run. Pruning happens before execution, under scheduler ownership.
type QueueBudgetTask interface {
	Task
	SetQueueBudget(QueueBudget)
	QueueBudget() QueueBudget
	PruneQueued(time.Time) (Task, int, error)
}

func (t *queuedTask) prune(now time.Time) (Task, int, error) {
	if group, ok := t.Task.(QueueBudgetTask); ok {
		survivor, dropped, cause := group.PruneQueued(now)
		if survivor != nil {
			budget := survivor.(QueueBudgetTask).QueueBudget()
			t.enqueueTime, t.maxWait = budget.EnqueueTime, budget.MaxWait
		}
		return survivor, dropped, cause
	}
	return pruneCanceled(t.Task)
}
