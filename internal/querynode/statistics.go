package querynode

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
)

// statisticOnSegments performs statistic on listed segments
// all segment ids are validated before calling this function
func statisticOnSegments(replica ReplicaInterface, segType segmentType, segIDs []UniqueID) ([]map[string]interface{}, error) {
	// results variables
	searchResults := make([]map[string]interface{}, len(segIDs))
	errs := make([]error, len(segIDs))

	// calling segment search in goroutines
	var wg sync.WaitGroup
	for i, segID := range segIDs {
		wg.Add(1)
		go func(segID UniqueID, i int) {
			defer wg.Done()
			seg, err := replica.getSegmentByID(segID, segType)
			if err != nil {
				log.Error(err.Error()) // should not happen but still ignore it since the result is still correct
				return
			}
			// record search time
			//tr := timerecord.NewTimeRecorder("searchOnSegments")
			count := seg.getRealCount()
			if count < 0 {
				errs[i] = fmt.Errorf("segment %d has nil ptr", segID)
			}
			errs[i] = err
			searchResults[i] = map[string]interface{}{
				"row_count": count,
			}
			// update metrics
			//metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			//	metrics.SearchLabel,
			//	metrics.SealedSegmentLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
		}(segID, i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return searchResults, nil
}

// statistic will do statistics on the historical segments the target segments in historical.
// if segIDs is not specified, it will search on all the historical segments specified by partIDs.
// if segIDs is specified, it will only search on the segments specified by the segIDs.
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func statisticHistorical(ctx context.Context, replica ReplicaInterface, collID UniqueID, partIDs []UniqueID, segIDs []UniqueID) ([]map[string]interface{}, []UniqueID, []UniqueID, error) {
	searchPartIDs, searchSegmentIDs, err := validateOnHistoricalReplica(ctx, replica, collID, partIDs, segIDs)
	if err != nil {
		return nil, searchSegmentIDs, searchPartIDs, err
	}
	searchResults, err := statisticOnSegments(replica, segmentTypeSealed, searchSegmentIDs)
	return searchResults, searchPartIDs, searchSegmentIDs, err
}

// statisticStreaming will do statistics all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func statisticStreaming(ctx context.Context, replica ReplicaInterface, collID UniqueID, partIDs []UniqueID, vChannel Channel) ([]map[string]interface{}, []UniqueID, []UniqueID, error) {
	searchPartIDs, searchSegmentIDs, err := validateOnStreamReplica(ctx, replica, collID, partIDs, vChannel)
	if err != nil {
		return nil, searchSegmentIDs, searchPartIDs, err
	}
	searchResults, err := statisticOnSegments(replica, segmentTypeGrowing, searchSegmentIDs)
	return searchResults, searchPartIDs, searchSegmentIDs, err
}
