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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewNodeItem tests the creation of NodeItem
func TestNewNodeItem(t *testing.T) {
	// Test creating a new node item
	nodeID := int64(100)
	currentScore := 50
	item := NewNodeItem(currentScore, nodeID)

	assert.Equal(t, nodeID, item.NodeID)
	assert.Equal(t, float64(currentScore), item.GetCurrentScore())
	assert.Equal(t, float64(0), item.GetAssignedScore())
}

// TestNodeItem_GetPriority tests priority calculation
func TestNodeItem_GetPriority(t *testing.T) {
	item := NewNodeItem(100, 1)
	item.SetAssignedScore(50)

	// Priority should be CurrentScore - AssignedScore = 100 - 50 = 50
	priority := item.getPriority()
	assert.Equal(t, 50, priority)
}

// TestNodeItem_GetPriorityWithCurrentScoreDelta tests priority calculation with delta
func TestNodeItem_GetPriorityWithCurrentScoreDelta(t *testing.T) {
	item := NewNodeItem(100, 1)
	item.SetAssignedScore(50)

	// Current priority: 100 - 50 = 50
	// With delta 20: (100 + 20) - 50 = 70
	priorityWithDelta := item.GetPriorityWithCurrentScoreDelta(20)
	assert.Equal(t, 70, priorityWithDelta)
}

// TestNodeItem_AddCurrentScoreDelta tests adding delta to current score
func TestNodeItem_AddCurrentScoreDelta(t *testing.T) {
	item := NewNodeItem(100, 1)
	item.SetAssignedScore(50)

	// Initial priority: 100 - 50 = 50
	assert.Equal(t, 50, item.getPriority())

	// Add delta
	item.AddCurrentScoreDelta(30)

	// New current score: 100 + 30 = 130
	assert.Equal(t, float64(130), item.GetCurrentScore())
	// New priority: 130 - 50 = 80
	assert.Equal(t, 80, item.getPriority())
}

// TestNodeItem_SetAssignedScore tests setting assigned score
func TestNodeItem_SetAssignedScore(t *testing.T) {
	item := NewNodeItem(100, 1)

	// Set assigned score
	item.SetAssignedScore(40)

	assert.Equal(t, float64(40), item.GetAssignedScore())
	// Priority: 100 - 40 = 60
	assert.Equal(t, 60, item.getPriority())
}

// TestNodeItem_SetPriority tests that setPriority panics
func TestNodeItem_SetPriority(t *testing.T) {
	item := NewNodeItem(100, 1)

	// setPriority should panic
	assert.Panics(t, func() {
		item.setPriority(50)
	})
}

// TestNodeItem_String tests string representation
func TestNodeItem_String(t *testing.T) {
	item := NewNodeItem(100, 1)
	item.SetAssignedScore(50)

	str := item.String()
	assert.Contains(t, str, "NodeID: 1")
	assert.Contains(t, str, "AssignedScore:")
	assert.Contains(t, str, "CurrentScore:")
	assert.Contains(t, str, "Priority:")
}

// TestNodeItem_NegativeScores tests behavior with negative score changes
func TestNodeItem_NegativeScores(t *testing.T) {
	item := NewNodeItem(100, 1)
	item.SetAssignedScore(50)

	// Subtract from current score
	item.AddCurrentScoreDelta(-30)

	// New current score: 100 - 30 = 70
	assert.Equal(t, float64(70), item.GetCurrentScore())
	// New priority: 70 - 50 = 20
	assert.Equal(t, 20, item.getPriority())
}

// TestNodeItem_FloatingPointPriority tests ceiling behavior in priority calculation
func TestNodeItem_FloatingPointPriority(t *testing.T) {
	item := NewNodeItem(100, 1)

	// Set assigned score that results in fractional priority
	item.AssignedScore = 50.7

	// Priority: ceil(100 - 50.7) = ceil(49.3) = 50
	priority := item.getPriority()
	assert.Equal(t, 50, priority)
}

// TestNodeItem_ZeroScores tests behavior with zero scores
func TestNodeItem_ZeroScores(t *testing.T) {
	item := NewNodeItem(0, 1)

	assert.Equal(t, float64(0), item.GetCurrentScore())
	assert.Equal(t, float64(0), item.GetAssignedScore())
	assert.Equal(t, 0, item.getPriority())
}
