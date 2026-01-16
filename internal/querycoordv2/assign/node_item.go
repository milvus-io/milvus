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

package assign

import (
	"fmt"
	"math"
)

// NodeItem represents a node with score information for assignment
type NodeItem struct {
	BaseItem
	fmt.Stringer
	NodeID        int64
	AssignedScore float64
	CurrentScore  float64
}

// NewNodeItem creates a new NodeItem with the given current score and node ID
func NewNodeItem(currentScore int, nodeID int64) NodeItem {
	return NodeItem{
		BaseItem:     BaseItem{},
		NodeID:       nodeID,
		CurrentScore: float64(currentScore),
	}
}

func (b *NodeItem) getPriority() int {
	// if node lacks more score between assignedScore and currentScore, then higher priority
	return int(math.Ceil(b.CurrentScore - b.AssignedScore))
}

func (b *NodeItem) setPriority(priority int) {
	panic("not supported, use updatePriority instead")
}

func (b *NodeItem) GetPriorityWithCurrentScoreDelta(delta float64) int {
	return int(math.Ceil((b.CurrentScore + delta) - b.AssignedScore))
}

func (b *NodeItem) GetCurrentScore() float64 {
	return b.CurrentScore
}

func (b *NodeItem) AddCurrentScoreDelta(delta float64) {
	b.CurrentScore += delta
	b.priority = b.getPriority()
}

func (b *NodeItem) GetAssignedScore() float64 {
	return b.AssignedScore
}

func (b *NodeItem) SetAssignedScore(delta float64) {
	b.AssignedScore += delta
	b.priority = b.getPriority()
}

func (b *NodeItem) String() string {
	return fmt.Sprintf("{NodeID: %d, AssignedScore: %f, CurrentScore: %f, Priority: %d}", b.NodeID, b.AssignedScore, b.CurrentScore, b.priority)
}
