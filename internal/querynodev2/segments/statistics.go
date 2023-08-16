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
)

// SegmentStats struct for segment statistics.
type SegmentStats struct {
	SegmentID int64
	RowCount  int64
}

// statisticOnSegments performs statistic on listed segments
// all segment ids are validated before calling this function
func statisticOnSegments(ctx context.Context, segments []Segment, segType SegmentType) ([]SegmentStats, error) {
	// results variables
	results := make([]SegmentStats, 0, len(segments))
	resultCh := make(chan SegmentStats, len(segments))

	// fetch seg statistics in goroutines
	var wg sync.WaitGroup
	for i, segment := range segments {
		wg.Add(1)
		go func(segment Segment, i int) {
			defer wg.Done()
			resultCh <- SegmentStats{
				SegmentID: segment.ID(),
				RowCount:  segment.RowNum(),
			}
		}(segment, i)
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
func StatisticsHistorical(ctx context.Context, manager *Manager, collID int64, partIDs []int64, segIDs []int64) ([]SegmentStats, []Segment, error) {
	segments, err := validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	result, err := statisticOnSegments(ctx, segments, SegmentTypeSealed)
	return result, segments, err
}

// StatisticStreaming will do statistics all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func StatisticStreaming(ctx context.Context, manager *Manager, collID int64, partIDs []int64, segIDs []int64) ([]SegmentStats, []Segment, error) {
	segments, err := validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	result, err := statisticOnSegments(ctx, segments, SegmentTypeGrowing)
	return result, segments, err
}
