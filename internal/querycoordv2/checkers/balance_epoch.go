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

package checkers

import (
	"context"
	"maps"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type balanceEpochWork struct {
	task         task.Task
	admittedAt   time.Time
	queueTimeout time.Duration
}

// Epochs bound planning, not remote transactions. A failed operation does not
// cancel its siblings. QN-reported loading remains reserved after local timeout.
type balanceEpoch struct {
	id    uint64
	works []balanceEpochWork
}

type recoveryBlockedRGsKey struct{}

// WithRecoveryBlockedRGs supplies the missing-channel/segment diff from the
// ordered recovery checks. An explicit empty set means recovery found no gaps;
// pending balance Grow actions alone must not be classified as recovery.
func WithRecoveryBlockedRGs(ctx context.Context, blocked map[string]struct{}) context.Context {
	return context.WithValue(ctx, recoveryBlockedRGsKey{}, maps.Clone(blocked))
}

func recoveryBlockedRG(ctx context.Context, rg string) bool {
	blocked, _ := ctx.Value(recoveryBlockedRGsKey{}).(map[string]struct{})
	_, pending := blocked[rg]
	return pending
}

func (b *BalanceChecker) planningBlocks(ctx context.Context, replica *meta.Replica, isStoppingBalance bool) bool {
	if b.epochBlocks(replica.GetResourceGroup()) {
		return true
	}
	if isStoppingBalance {
		return false
	}
	if _, ordered := ctx.Value(recoveryBlockedRGsKey{}).(map[string]struct{}); ordered {
		return recoveryBlockedRG(ctx, replica.GetResourceGroup())
	}
	// Older direct callers do not provide a recovery diff. Keep their
	// conservative pending-load guard; the ordered controller always supplies
	// an explicit set and therefore never uses this as recovery classification.
	return b.hasPendingLoads(replica)
}

func (b *BalanceChecker) epochBlocks(rg string) bool {
	epoch := b.epochs[rg]
	return epoch != nil && epoch.id != b.epochSequence
}

func (b *BalanceChecker) trackEpochWork(t task.Task) {
	if b.epochs == nil {
		b.epochs = make(map[string]*balanceEpoch)
	}
	rg := t.ResourceGroup()
	epoch := b.epochs[rg]
	if epoch == nil {
		epoch = &balanceEpoch{id: b.epochSequence}
		b.epochs[rg] = epoch
	}
	timeout := paramtable.Get().QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond)
	if _, channel := t.(*task.ChannelTask); channel {
		timeout = paramtable.Get().QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond)
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	epoch.works = append(epoch.works, balanceEpochWork{task: t, admittedAt: time.Now(), queueTimeout: timeout})
}

func (b *BalanceChecker) advanceEpochs(now time.Time) {
	for rg, epoch := range b.epochs {
		remaining := epoch.works[:0]
		for _, work := range epoch.works {
			switch work.task.Status() {
			case task.TaskStatusSucceeded, task.TaskStatusFailed, task.TaskStatusCanceled:
				continue
			}
			if err := work.task.Context().Err(); err != nil {
				work.task.Cancel(err)
				continue
			}
			// Execution arms its own per-action deadline. Bound admission waiting
			// separately so a task which never obtains an executor cannot pin an RG.
			if _, armed := work.task.Context().Deadline(); !armed && work.queueTimeout > 0 && now.Sub(work.admittedAt) >= work.queueTimeout {
				work.task.Cancel(merr.Wrap(context.DeadlineExceeded, "balance task exceeded scheduler queue wait"))
				continue
			}
			remaining = append(remaining, work)
		}
		epoch.works = remaining
		if len(remaining) == 0 {
			delete(b.epochs, rg)
		}
	}
}

// This compatibility gate cannot distinguish recovery from balance loading.
func (b *BalanceChecker) hasPendingLoads(replica *meta.Replica) bool {
	nodes := replica.GetNodes()
	if inspector, ok := b.scheduler.(interface {
		PendingSegmentLoads() map[int64][]*querypb.LoadingSegmentInfo
	}); ok {
		pending := inspector.PendingSegmentLoads()
		for _, nodeID := range nodes {
			for _, load := range pending[nodeID] {
				if load.GetCollectionId() == replica.GetCollectionID() && load.GetNumRows() > 0 {
					return true
				}
			}
		}
	}
	for _, nodeID := range nodes {
		if node := b.nodeManager.Get(nodeID); node != nil {
			for _, load := range node.LoadResourceReport().GetLoadingSegments() {
				if load.GetCollectionId() == replica.GetCollectionID() {
					return true
				}
			}
		}
	}
	return false
}
