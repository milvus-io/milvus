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

package datacoord

import (
	"sync"
	"sync/atomic"
)

var (
	buildIndexCh     chan UniqueID
	statsTaskCh      chan UniqueID
	statsBuildCh     chan UniqueID
	buildIndexChOnce sync.Once
	statsTaskChOnce  sync.Once
	statsBuildChOnce sync.Once

	// buildIndexConsumerDone holds the done channel (<-chan struct{}) of the
	// index inspector that drains buildIndexCh.
	buildIndexConsumerDone atomic.Value
	// statsBuildConsumerDone holds the done channel (<-chan struct{}) of the
	// stats inspector that drains statsBuildCh.
	statsBuildConsumerDone atomic.Value
)

func getBuildIndexChSingleton() chan UniqueID {
	buildIndexChOnce.Do(func() {
		buildIndexCh = make(chan UniqueID, 1024)
	})

	return buildIndexCh
}

// setBuildIndexConsumer records that buildIndexCh is drained until done is closed.
func setBuildIndexConsumer(done <-chan struct{}) {
	buildIndexConsumerDone.Store(done)
}

// notifyCollectionIndexBuild asks the index inspector to enqueue every segment
// of the collection that has no index task yet. It never blocks; a dropped
// notification is covered by the inspector's periodic reconcile.
func notifyCollectionIndexBuild(notifyIndexChan chan<- UniqueID, collectionID UniqueID) {
	select {
	case notifyIndexChan <- collectionID:
	default:
	}
}

// notifySegmentIndexBuild asks the index inspector to build the segments'
// missing indexes. While an inspector drains buildIndexCh, a full channel makes
// it wait for room instead of dropping the segment; the inspector drains on a
// dedicated goroutine, so the wait is short. Without a running inspector it
// drops, since the next inspector start scans every unindexed segment anyway.
func notifySegmentIndexBuild(segIDs ...UniqueID) {
	ch := getBuildIndexChSingleton()
	done, _ := buildIndexConsumerDone.Load().(<-chan struct{})
	for _, segID := range segIDs {
		select {
		case ch <- segID:
			continue
		default:
		}
		if done == nil {
			return
		}
		select {
		case ch <- segID:
		case <-done:
			return
		}
	}
}

func getStatsTaskChSingleton() chan UniqueID {
	statsTaskChOnce.Do(func() {
		statsTaskCh = make(chan UniqueID, 1024)
	})
	return statsTaskCh
}

// getStatsBuildChSingleton carries segments that may need text or JSON key
// stats to the stats inspector. statsTaskCh is the sort-compaction trigger's.
func getStatsBuildChSingleton() chan UniqueID {
	statsBuildChOnce.Do(func() {
		statsBuildCh = make(chan UniqueID, 1024)
	})
	return statsBuildCh
}

// setStatsBuildConsumer records that statsBuildCh is drained until done is closed.
func setStatsBuildConsumer(done <-chan struct{}) {
	statsBuildConsumerDone.Store(done)
}

// notifyCollectionStatsBuild asks the stats inspector to check every segment of
// the collection. It never blocks; a dropped notification is covered by the
// inspector's periodic reconcile.
func notifyCollectionStatsBuild(notifyStatsChan chan<- UniqueID, collectionID UniqueID) {
	select {
	case notifyStatsChan <- collectionID:
	default:
	}
}

// notifySegmentStatsBuild asks the stats inspector to submit the segments'
// missing text and JSON key stats tasks. While an inspector drains
// statsBuildCh, a full channel makes it wait for room instead of dropping the
// segment; the inspector drains on a dedicated goroutine, so the wait is short.
// Without a running inspector it drops, since the next inspector start scans
// every segment anyway.
func notifySegmentStatsBuild(segIDs ...UniqueID) {
	ch := getStatsBuildChSingleton()
	done, _ := statsBuildConsumerDone.Load().(<-chan struct{})
	for _, segID := range segIDs {
		select {
		case ch <- segID:
			continue
		default:
		}
		if done == nil {
			return
		}
		select {
		case ch <- segID:
		case <-done:
			return
		}
	}
}
