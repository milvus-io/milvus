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

package segments

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
)

// SegmentStats struct for segment statistics.
type SegmentStats struct {
	SegmentID int64
	RowCount  int64
}

// statisticOnSegments performs statistic on listed segments
// all segment ids are validated before calling this function
func statisticOnSegments(ctx context.Context, manager *Manager, segType SegmentType, segIDs []int64) ([]SegmentStats, error) {
	// results variables
	results := make([]SegmentStats, 0, len(segIDs))
	resultCh := make(chan SegmentStats, len(segIDs))

	// fetch seg statistics in goroutines
	var wg sync.WaitGroup
	for i, segID := range segIDs {
		wg.Add(1)
		go func(segID int64, i int) {
			defer wg.Done()
			seg := manager.Segment.GetWithType(segID, segType)
			if seg == nil {
				log.Warn("segment released while get statistics")
				return
			}

			resultCh <- SegmentStats{
				SegmentID: segID,
				RowCount:  seg.RowNum(),
			}
		}(segID, i)
	}
	wg.Wait()
	close(resultCh)
	for result := range resultCh {
		results = append(results, result)
	}

	return results, nil
}

// statistic will do statistics on the historical segments the target segments in historical.
// if segIDs is not specified, it will search on all the historical segments specified by partIDs.
// if segIDs is specified, it will only search on the segments specified by the segIDs.
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func StatisticsHistorical(ctx context.Context, manager *Manager, collID int64, partIDs []int64, segIDs []int64) ([]SegmentStats, []int64, []int64, error) {
	var err error
	var result []SegmentStats
	var searchSegmentIDs []int64
	var searchPartIDs []int64
	searchPartIDs, searchSegmentIDs, err = validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return result, searchSegmentIDs, searchPartIDs, err
	}
	result, err = statisticOnSegments(ctx, manager, SegmentTypeSealed, searchSegmentIDs)
	return result, searchPartIDs, searchSegmentIDs, err
}

// StatisticStreaming will do statistics all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func StatisticStreaming(ctx context.Context, manager *Manager, collID int64, partIDs []int64, segIDs []int64) ([]SegmentStats, []int64, []int64, error) {
	var err error
	var result []SegmentStats
	var searchSegmentIDs []int64
	var searchPartIDs []int64
	searchPartIDs, searchSegmentIDs, err = validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return result, searchSegmentIDs, searchPartIDs, err
	}
	result, err = statisticOnSegments(ctx, manager, SegmentTypeGrowing, searchSegmentIDs)
	return result, searchPartIDs, searchSegmentIDs, err
}
