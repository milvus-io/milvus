package delegator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type PruneInfo struct {
	filterRatio float64
}

func PruneSegments(ctx context.Context,
	partitionStats map[UniqueID]*storage.PartitionStatsSnapshot,
	searchReq *internalpb.SearchRequest,
	queryReq *internalpb.RetrieveRequest,
	schema *schemapb.CollectionSchema,
	sealedSegments []SnapshotItem,
	info PruneInfo,
) {
	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "segmentPrune")
	defer span.End()
	if partitionStats == nil {
		return
	}
	// 1. select collection, partitions and expr
	clusteringKeyField := clustering.GetClusteringKeyField(schema)
	if clusteringKeyField == nil {
		// no need to prune
		return
	}
	tr := timerecord.NewTimeRecorder("PruneSegments")
	var collectionID int64
	var expr []byte
	var partitionIDs []int64
	if searchReq != nil {
		collectionID = searchReq.CollectionID
		expr = searchReq.GetSerializedExprPlan()
		partitionIDs = searchReq.GetPartitionIDs()
	} else {
		collectionID = queryReq.CollectionID
		expr = queryReq.GetSerializedExprPlan()
		partitionIDs = queryReq.GetPartitionIDs()
	}

	filteredSegments := make(map[UniqueID]struct{}, 0)
	pruneType := "scalar"
	// currently we only prune based on one column
	if typeutil.IsVectorType(clusteringKeyField.GetDataType()) {
		// parse searched vectors
		var vectorsHolder commonpb.PlaceholderGroup
		err := proto.Unmarshal(searchReq.GetPlaceholderGroup(), &vectorsHolder)
		if err != nil || len(vectorsHolder.GetPlaceholders()) == 0 {
			return
		}
		vectorsBytes := vectorsHolder.GetPlaceholders()[0].GetValues()
		// parse dim
		dimStr, err := funcutil.GetAttrByKeyFromRepeatedKV(common.DimKey, clusteringKeyField.GetTypeParams())
		if err != nil {
			return
		}
		dimValue, err := strconv.ParseInt(dimStr, 10, 64)
		if err != nil {
			return
		}
		for _, partStats := range partitionStats {
			FilterSegmentsByVector(partStats, searchReq, vectorsBytes, dimValue, clusteringKeyField, filteredSegments, info.filterRatio)
		}
		pruneType = "vector"
	} else {
		// 0. parse expr from plan
		plan := planpb.PlanNode{}
		err := proto.Unmarshal(expr, &plan)
		if err != nil {
			log.Ctx(ctx).Error("failed to unmarshall serialized expr from bytes, failed the operation")
			return
		}
		exprPb, err := exprutil.ParseExprFromPlan(&plan)
		if err != nil {
			log.Ctx(ctx).Error("failed to parse expr from plan, failed the operation")
			return
		}

		// 1. parse expr for prune
		expr, err := ParseExpr(exprPb, NewParseContext(clusteringKeyField.GetFieldID(), clusteringKeyField.GetDataType()))
		if err != nil {
			log.Ctx(ctx).RatedWarn(10, "failed to parse expr for segment prune, fallback to common search/query", zap.Error(err))
			return
		}

		// 2. prune segments by scalar field
		targetSegmentStats := make([]storage.SegmentStats, 0, 32)
		targetSegmentIDs := make([]int64, 0, 32)
		if len(partitionIDs) > 0 {
			for _, partID := range partitionIDs {
				partStats, exist := partitionStats[partID]
				if exist && partStats != nil {
					for segID, segStat := range partStats.SegmentStats {
						targetSegmentIDs = append(targetSegmentIDs, segID)
						targetSegmentStats = append(targetSegmentStats, segStat)
					}
				}
			}
		} else {
			for _, partStats := range partitionStats {
				if partStats != nil {
					for segID, segStat := range partStats.SegmentStats {
						targetSegmentIDs = append(targetSegmentIDs, segID)
						targetSegmentStats = append(targetSegmentStats, segStat)
					}
				}
			}
		}

		PruneByScalarField(expr, targetSegmentStats, targetSegmentIDs, filteredSegments)
	}

	// 2. remove filtered segments from sealed segment list
	if len(filteredSegments) > 0 {
		realFilteredSegments := 0
		totalSegNum := 0
		minSegmentCount := math.MaxInt
		maxSegmentCount := 0
		for idx, item := range sealedSegments {
			newSegments := make([]SegmentEntry, 0)
			totalSegNum += len(item.Segments)
			for _, segment := range item.Segments {
				_, exist := filteredSegments[segment.SegmentID]
				if exist {
					realFilteredSegments++
				} else {
					newSegments = append(newSegments, segment)
				}
			}
			item.Segments = newSegments
			sealedSegments[idx] = item
			segmentCount := len(item.Segments)
			if segmentCount > maxSegmentCount {
				maxSegmentCount = segmentCount
			}
			if segmentCount < minSegmentCount {
				minSegmentCount = segmentCount
			}
		}
		bias := 1.0
		if maxSegmentCount != 0 && minSegmentCount != math.MaxInt {
			bias = float64(maxSegmentCount) / float64(minSegmentCount)
		}
		metrics.QueryNodeSegmentPruneBias.
			WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				fmt.Sprint(collectionID),
				pruneType,
			).Set(bias)

		filterRatio := float32(realFilteredSegments) / float32(totalSegNum)
		metrics.QueryNodeSegmentPruneRatio.
			WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				fmt.Sprint(collectionID),
				pruneType,
			).Set(float64(filterRatio))
		log.Ctx(ctx).Debug("Pruned segment for search/query",
			zap.Int("filtered_segment_num[stats]", len(filteredSegments)),
			zap.Int("filtered_segment_num[excluded]", realFilteredSegments),
			zap.Int("total_segment_num", totalSegNum),
			zap.Float32("filtered_ratio", filterRatio),
		)
	}

	metrics.QueryNodeSegmentPruneLatency.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		fmt.Sprint(collectionID),
		pruneType).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Ctx(ctx).Debug("Pruned segment for search/query",
		zap.Duration("duration", tr.ElapseSpan()))
}

type segmentDisStruct struct {
	segmentID UniqueID
	distance  float32
	rows      int // for keep track of sufficiency of topK
}

func FilterSegmentsByVector(partitionStats *storage.PartitionStatsSnapshot,
	searchReq *internalpb.SearchRequest,
	vectorBytes [][]byte,
	dim int64,
	keyField *schemapb.FieldSchema,
	filteredSegments map[UniqueID]struct{},
	filterRatio float64,
) {
	// 1. calculate vectors' distances
	neededSegments := make(map[UniqueID]struct{})
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
						log.Error("calculate distance error", zap.Error(disErr))
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
				return segmentsToSearch[i].distance < segmentsToSearch[j].distance
			})
		case distance.IP, distance.COSINE:
			sort.SliceStable(segmentsToSearch, func(i, j int) bool {
				return segmentsToSearch[i].distance > segmentsToSearch[j].distance
			})
		}

		// 3. filtered non-target segments
		segmentCount := len(segmentsToSearch)
		targetSegNum := int(math.Sqrt(float64(segmentCount)) * filterRatio)
		if targetSegNum > segmentCount {
			log.Debug("Warn! targetSegNum is larger or equal than segmentCount, no prune effect at all",
				zap.Int("targetSegNum", targetSegNum),
				zap.Int("segmentCount", segmentCount),
				zap.Float64("filterRatio", filterRatio))
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

func FilterSegmentsOnScalarField(partitionStats *storage.PartitionStatsSnapshot,
	targetRanges []*exprutil.PlanRange,
	keyField *schemapb.FieldSchema,
	filteredSegments map[UniqueID]struct{},
) {
	// 1. try to filter segments
	overlap := func(min storage.ScalarFieldValue, max storage.ScalarFieldValue) bool {
		for _, tRange := range targetRanges {
			switch keyField.DataType {
			case schemapb.DataType_Int8:
				targetRange := tRange.ToIntRange()
				statRange := exprutil.NewIntRange(int64(min.GetValue().(int8)), int64(max.GetValue().(int8)), true, true)
				return exprutil.IntRangeOverlap(targetRange, statRange)
			case schemapb.DataType_Int16:
				targetRange := tRange.ToIntRange()
				statRange := exprutil.NewIntRange(int64(min.GetValue().(int16)), int64(max.GetValue().(int16)), true, true)
				return exprutil.IntRangeOverlap(targetRange, statRange)
			case schemapb.DataType_Int32:
				targetRange := tRange.ToIntRange()
				statRange := exprutil.NewIntRange(int64(min.GetValue().(int32)), int64(max.GetValue().(int32)), true, true)
				return exprutil.IntRangeOverlap(targetRange, statRange)
			case schemapb.DataType_Int64:
				targetRange := tRange.ToIntRange()
				statRange := exprutil.NewIntRange(min.GetValue().(int64), max.GetValue().(int64), true, true)
				return exprutil.IntRangeOverlap(targetRange, statRange)
			// todo: add float/double pruner
			case schemapb.DataType_String, schemapb.DataType_VarChar:
				targetRange := tRange.ToStrRange()
				statRange := exprutil.NewStrRange(min.GetValue().(string), max.GetValue().(string), true, true)
				return exprutil.StrRangeOverlap(targetRange, statRange)
			}
		}
		return false
	}
	for segID, segStats := range partitionStats.SegmentStats {
		for _, fieldStat := range segStats.FieldStats {
			if keyField.FieldID == fieldStat.FieldID && !overlap(fieldStat.Min, fieldStat.Max) {
				filteredSegments[segID] = struct{}{}
			}
		}
	}
}
