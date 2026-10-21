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

package mvcc

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// readProofManager projects only published, complete ACK prefixes. Append-time
// query frontiers can include writes beyond the confirmed prefix and therefore
// cannot be independently paired with a confirmation timestamp.
type readProofManager struct {
	mu        sync.Mutex
	frontiers *QueryMVCCManager
	through   uint64
	changed   chan struct{}
}

func newReadProofManager() *readProofManager {
	return &readProofManager{frontiers: NewQueryMVCCManager(0), changed: make(chan struct{})}
}

func (m *readProofManager) applyRecoveryBarrier(channel string, tick uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frontiers.ApplyRecoveryBarrier(channel, tick)
	// Only PublishReadProof certifies a complete prefix for the current owner.
}

// PublishReadProof runs after WAB publication in the serialized sync operator.
// Messages are successful appends in ACK timestamp order. Transaction bodies
// do not change frontiers until their CommitTxn is included in the prefix.
// Idle confirmations refresh the certificate without advancing executor MVCC.
func (cm *MVCCManager) PublishReadProof(messages []message.ImmutableMessage, confirmation message.ImmutableMessage) {
	m := cm.readProof
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range messages {
		m.frontiers.UpdateMVCC(msg)
	}
	m.frontiers.UpdateMVCC(confirmation)
	m.through = confirmation.TimeTick()
	close(m.changed)
	m.changed = make(chan struct{})
}

// WaitReadProof captures both projections and their complete-prefix certificate
// under one lock. No query consumer needs to apply the certificate timestamp;
// it only needs the returned Growing and Transform frontiers.
func (cm *MVCCManager) WaitReadProof(ctx context.Context, channel string, minimum uint64) (QueryVChannelMVCC, uint64, error) {
	m := cm.readProof
	for {
		m.mu.Lock()
		frontier := m.frontiers.GetQueryMVCCOfVChannel(channel)
		through, changed := m.through, m.changed
		m.mu.Unlock()
		if through >= minimum && max(frontier.GrowingTimetick, frontier.TransformingTimetick) <= through &&
			(frontier.GrowingTimetick != 0 || frontier.TransformingTimetick != 0) {
			return frontier, through, nil
		}
		select {
		case <-ctx.Done():
			return QueryVChannelMVCC{}, 0, ctx.Err()
		case <-changed:
		}
	}
}
