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

package task

type BalanceEpochMeta struct {
	ResourceGroup string
	LeaderTerm    uint64
	Sequence      uint64
}

type BalanceAdmissionReason int

const (
	BalanceAdmissionAccepted BalanceAdmissionReason = iota
	BalanceAdmissionDuplicate
	BalanceAdmissionSourceGone
	BalanceAdmissionLeaderMissing
	BalanceAdmissionReplicaChanged
	BalanceAdmissionRGChanged
	BalanceAdmissionTargetChanged
	BalanceAdmissionNodeIneligible
	BalanceAdmissionBudgetExhausted
	BalanceAdmissionStaleEpoch
	BalanceAdmissionInternalError
)

type BalanceAdmissionResult struct {
	TaskID          int64
	Reason          BalanceAdmissionReason
	Err             error
	PendingRevision BalancePendingRevision
}

type BalancePendingRevision struct {
	ResourceGroup string
	Epoch         BalanceEpochMeta
	Revision      uint64
	EpochRevision uint64
}

func (r BalancePendingRevision) EffectiveRevision() uint64 {
	if r.EpochRevision > r.Revision {
		return 0
	}
	return r.Revision - r.EpochRevision
}

type BalanceAdmissionValidator func() BalanceAdmissionReason

type BalanceTaskAdmitter interface {
	AdmitBalanceTask(task Task, validate BalanceAdmissionValidator) BalanceAdmissionResult
}

// BalanceTaskGenerationAdmitter atomically compares the expected pending
// generation and commits the task under the scheduler pending lock. The
// generation combines the RG-scoped counter with a topology-independent fence
// for NilReplica cleanup work whose action-node RG mapping may change.
// The epoch-owned portion counts only expected generation-aware commits and is
// subtracted so independent admissions from the same wave do not invalidate
// each other. Legacy adds, removals, and failures remain visible mutations.
type BalanceTaskGenerationAdmitter interface {
	AdmitBalanceTaskAtPendingRevision(task Task, expected BalancePendingRevision, validate BalanceAdmissionValidator) BalanceAdmissionResult
}

func (r BalanceAdmissionReason) String() string {
	switch r {
	case BalanceAdmissionAccepted:
		return "accepted"
	case BalanceAdmissionDuplicate:
		return "duplicate"
	case BalanceAdmissionSourceGone:
		return "source_gone"
	case BalanceAdmissionLeaderMissing:
		return "leader_missing"
	case BalanceAdmissionReplicaChanged:
		return "replica_changed"
	case BalanceAdmissionRGChanged:
		return "resource_group_changed"
	case BalanceAdmissionTargetChanged:
		return "target_changed"
	case BalanceAdmissionNodeIneligible:
		return "node_ineligible"
	case BalanceAdmissionBudgetExhausted:
		return "budget_exhausted"
	case BalanceAdmissionStaleEpoch:
		return "stale_epoch"
	default:
		return "internal_error"
	}
}

func (r BalanceAdmissionReason) InvalidatesScope() bool {
	return r == BalanceAdmissionReplicaChanged ||
		r == BalanceAdmissionRGChanged ||
		r == BalanceAdmissionTargetChanged ||
		r == BalanceAdmissionNodeIneligible ||
		r == BalanceAdmissionStaleEpoch
}
