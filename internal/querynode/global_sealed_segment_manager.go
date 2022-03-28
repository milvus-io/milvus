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

package querynode

import (
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

// globalSealedSegmentManager manages the globalSealedSegments
type globalSealedSegmentManager struct {
	collectionID         UniqueID
	mu                   sync.Mutex                        // guards globalSealedSegments
	globalSealedSegments map[UniqueID]*querypb.SegmentInfo // map[segmentID]SegmentInfo
}

// newGlobalSealedSegmentManager returns a new globalSealedSegmentManager
func newGlobalSealedSegmentManager(collectionID UniqueID) *globalSealedSegmentManager {
	return &globalSealedSegmentManager{
		collectionID:         collectionID,
		globalSealedSegments: make(map[UniqueID]*querypb.SegmentInfo),
	}
}

// addGlobalSegmentInfo adds a new segmentInfo
func (g *globalSealedSegmentManager) addGlobalSegmentInfo(segmentInfo *querypb.SegmentInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if segmentInfo.CollectionID != g.collectionID {
		log.Warn("Find mismatch collectionID when addGlobalSegmentInfo",
			zap.Any("manager collectionID", g.collectionID),
			zap.Any("segmentInfo collectionID", segmentInfo.CollectionID),
		)
	}
	g.globalSealedSegments[segmentInfo.SegmentID] = segmentInfo
}

// getGlobalSegmentIDs returns globalSealedSegments
func (g *globalSealedSegmentManager) getGlobalSegmentIDs() []UniqueID {
	g.mu.Lock()
	defer g.mu.Unlock()
	resIDs := make([]UniqueID, 0)
	for _, v := range g.globalSealedSegments {
		resIDs = append(resIDs, v.SegmentID)
	}
	return resIDs
}

// getGlobalSegmentIDsByPartitionIds returns globalSealedSegments by partitionIDs
func (g *globalSealedSegmentManager) getGlobalSegmentIDsByPartitionIds(partitionIDs []UniqueID) []UniqueID {
	g.mu.Lock()
	defer g.mu.Unlock()
	resIDs := make([]UniqueID, 0)
	for _, v := range g.globalSealedSegments {
		for _, partitionID := range partitionIDs {
			if v.PartitionID == partitionID {
				resIDs = append(resIDs, v.SegmentID)
			}
		}
	}
	return resIDs
}

// hasGlobalSealedSegment checks if globalSealedSegmentManager has globalSealedSegment by segmentID
func (g *globalSealedSegmentManager) hasGlobalSealedSegment(segmentID UniqueID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.globalSealedSegments[segmentID]
	return ok
}

// removeGlobalSealedSegmentInfo would remove globalSealSegment by segment
func (g *globalSealedSegmentManager) removeGlobalSealedSegmentInfo(segmentID UniqueID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.globalSealedSegments, segmentID)
}

// removeGlobalSegmentIDsByPartitionIds would remove globalSealedSegments by partitionIDs
func (g *globalSealedSegmentManager) removeGlobalSegmentIDsByPartitionIds(partitionIDs []UniqueID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, v := range g.globalSealedSegments {
		for _, partitionID := range partitionIDs {
			if v.PartitionID == partitionID {
				delete(g.globalSealedSegments, v.SegmentID)
			}
		}
	}
}

// close would free globalSealedSegmentManager
func (g *globalSealedSegmentManager) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.globalSealedSegments = make(map[UniqueID]*querypb.SegmentInfo)
}
