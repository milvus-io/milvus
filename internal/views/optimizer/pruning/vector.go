package pruning

import (
	"context"
	"math"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/distance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type segmentDisStruct struct {
	segmentID int64
	distance  float32
	rows      int // for keep track of sufficiency of topK
}

func FilterSegmentsByVector(ctx context.Context, partitionStats *storage.PartitionStatsSnapshot,
	searchReq *internalpb.SearchRequest,
	vectorBytes [][]byte,
	dim int64,
	keyField *schemapb.FieldSchema,
	filteredSegments map[int64]struct{},
	filterRatio float64,
) {
	// 1. calculate vectors' distances
	neededSegments := make(map[int64]struct{})
	for _, vecBytes := range vectorBytes {
		segmentsToSearch := make([]segmentDisStruct, 0)
		for segId, segStats := range partitionStats.SegmentStats {
			// here, we do not skip needed segments required by former query vector
			// meaning that repeated calculation will be carried and the larger the nq is
			// the more segments have to be included and prune effect will decline
			// 1. calculate distances from centroids
			for _, fieldStat := range segStats.FieldStats {
				if fieldStat.FieldID == keyField.GetFieldID() {
					if fieldStat.Centroids == nil || len(fieldStat.Centroids) == 0 {
						neededSegments[segId] = struct{}{}
						break
					}
					var dis []float32
					var disErr error
					switch keyField.GetDataType() {
					case schemapb.DataType_FloatVector:
						dis, disErr = clustering.CalcVectorDistance(dim, keyField.GetDataType(),
							vecBytes, fieldStat.Centroids[0].GetValue().([]float32), searchReq.GetMetricType())
					default:
						neededSegments[segId] = struct{}{}
						disErr = merr.WrapErrParameterInvalid(schemapb.DataType_FloatVector, keyField.GetDataType(),
							"Currently, pruning by cluster only support float_vector type")
					}
					// currently, we only support float vector and only one center one segment
					if disErr != nil {
						mlog.Error(ctx, "calculate distance error", mlog.Err(disErr))
						neededSegments[segId] = struct{}{}
						break
					}
					segmentsToSearch = append(segmentsToSearch, segmentDisStruct{
						segmentID: segId,
						distance:  dis[0],
						rows:      segStats.NumRows,
					})
					break
				}
			}
		}
		// 2. sort the distances
		switch searchReq.GetMetricType() {
		case distance.L2:
			sort.SliceStable(segmentsToSearch, func(i, j int) bool {
				if segmentsToSearch[i].distance == segmentsToSearch[j].distance {
					return segmentsToSearch[i].segmentID < segmentsToSearch[j].segmentID
				}
				return segmentsToSearch[i].distance < segmentsToSearch[j].distance
			})
		case distance.IP, distance.COSINE:
			sort.SliceStable(segmentsToSearch, func(i, j int) bool {
				if segmentsToSearch[i].distance == segmentsToSearch[j].distance {
					return segmentsToSearch[i].segmentID < segmentsToSearch[j].segmentID
				}
				return segmentsToSearch[i].distance > segmentsToSearch[j].distance
			})
		}

		// 3. filtered non-target segments
		segmentCount := len(segmentsToSearch)
		targetSegNum := int(math.Sqrt(float64(segmentCount)) * filterRatio)
		if targetSegNum > segmentCount {
			mlog.Debug(ctx, "Warn! targetSegNum is larger or equal than segmentCount, no prune effect at all",
				mlog.Int("targetSegNum", targetSegNum),
				mlog.Int("segmentCount", segmentCount),
				mlog.Float64("filterRatio", filterRatio))
			targetSegNum = segmentCount
		}
		optimizedRowCount := 0
		// set the last n - targetSegNum as being filtered
		for i := 0; i < segmentCount; i++ {
			optimizedRowCount += segmentsToSearch[i].rows
			neededSegments[segmentsToSearch[i].segmentID] = struct{}{}
			if int64(optimizedRowCount) >= searchReq.GetTopk() && i+1 >= targetSegNum {
				break
			}
		}
	}

	// 3. set not needed segments as removed
	for segId := range partitionStats.SegmentStats {
		if _, ok := neededSegments[segId]; !ok {
			filteredSegments[segId] = struct{}{}
		}
	}
}
