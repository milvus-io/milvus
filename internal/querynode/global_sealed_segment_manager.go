// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type globalSealedSegmentManager struct {
	collectionID         UniqueID
	mu                   sync.Mutex                        // guards globalSealedSegments
	globalSealedSegments map[UniqueID]*querypb.SegmentInfo // map[segmentID]SegmentInfo
}

func newGlobalSealedSegmentManager(collectionID UniqueID) *globalSealedSegmentManager {
	return &globalSealedSegmentManager{
		collectionID:         collectionID,
		globalSealedSegments: make(map[UniqueID]*querypb.SegmentInfo),
	}
}

func (g *globalSealedSegmentManager) addGlobalSegmentInfo(segmentInfo *querypb.SegmentInfo) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if segmentInfo.CollectionID != g.collectionID {
		return errors.New(fmt.Sprintln("mismatch collectionID when addGlobalSegmentInfo, ",
			"manager collectionID = ", g.collectionID, ", ",
			"segmentInfo collectionID = ", segmentInfo.CollectionID))
	}
	g.globalSealedSegments[segmentInfo.SegmentID] = segmentInfo
	return nil
}

func (g *globalSealedSegmentManager) getGlobalSegmentIDs() []UniqueID {
	g.mu.Lock()
	defer g.mu.Unlock()
	resIDs := make([]UniqueID, 0)
	for _, v := range g.globalSealedSegments {
		resIDs = append(resIDs, v.SegmentID)
	}
	return resIDs
}

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

func (g *globalSealedSegmentManager) removeGlobalSegmentInfo(segmentID UniqueID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.globalSealedSegments, segmentID)
}

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

func (g *globalSealedSegmentManager) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.globalSealedSegments = make(map[UniqueID]*querypb.SegmentInfo)
}
