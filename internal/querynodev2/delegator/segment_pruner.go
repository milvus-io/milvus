package delegator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/distance"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var gPrunePrinted = false

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
			log.Debug("partStats segment IDS", zap.Int64s("segmentIDs", maps.Keys(partStats.SegmentStats)))
			FilterSegmentsByVector(partStats, searchReq, vectorsBytes, dimValue, clusteringKeyField, filteredSegments, info.filterRatio)
		}
		pruneType = "vector"
	} else {
		// 0. parse expr from plan
		plan := planpb.PlanNode{}
		err := proto.Unmarshal(expr, &plan)
		if err != nil {
			mlog.Error(ctx, "failed to unmarshall serialized expr from bytes, failed the operation")
			return
		}
		exprPb, err := exprutil.ParseExprFromPlan(&plan)
		if err != nil {
			mlog.Error(ctx, "failed to parse expr from plan, failed the operation")
			return
		}

		// 1. parse expr for prune
		expr, err := ParseExpr(exprPb, NewParseContext(clusteringKeyField.GetFieldID(), clusteringKeyField.GetDataType()))
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(10), "failed to parse expr for segment prune, fallback to common search/query", mlog.Err(err))
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
				log.Debug("Filter segment id", zap.Int64("segmentID", segment.SegmentID), zap.Int64("NodeID", item.NodeID), zap.Bool("exist", exist))
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
			WithLabelValues(paramtable.GetStringNodeID(),
				fmt.Sprint(collectionID),
				pruneType,
			).Set(bias)

		filterRatio := float32(realFilteredSegments) / float32(totalSegNum)
		metrics.QueryNodeSegmentPruneRatio.
			WithLabelValues(paramtable.GetStringNodeID(),
				fmt.Sprint(collectionID),
				pruneType,
			).Set(float64(filterRatio))
		mlog.Debug(ctx, "Pruned segment for search/query",
			mlog.Int("filtered_segment_num[stats]", len(filteredSegments)),
			mlog.Int("filtered_segment_num[excluded]", realFilteredSegments),
			mlog.Int("total_segment_num", totalSegNum),
			mlog.Float32("filtered_ratio", filterRatio),
		)
	}

	metrics.QueryNodeSegmentPruneLatency.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(collectionID),
		pruneType).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	mlog.Debug(ctx, "Pruned segment for search/query",
		mlog.Duration("duration", tr.ElapseSpan()))
}

type segmentDisStruct struct {
	segmentID UniqueID
	distance  float32
	rows      int // for keep track of sufficiency of topK
}

func DistanceToCentroid(vectorFloat []float32,
	fieldStat storage.FieldStats,
	keyField *schemapb.FieldSchema,
	numRows int, metricType string,
	neededSegments map[UniqueID]struct{},
	segId UniqueID,
	segmentsToSearch map[string]segmentDisStruct,
	lock1 *sync.Mutex,
	lock2 *sync.Mutex,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	if fieldStat.Centroids == nil || len(fieldStat.Centroids) == 0 {
		lock1.Lock()
		neededSegments[segId] = struct{}{}
		lock1.Unlock()
		return
	}
	// Determine metric comparator once
	var computeDistance func(a, b []float32) float32
	if keyField.GetDataType() != schemapb.DataType_FloatVector {
		lock1.Lock()
		neededSegments[segId] = struct{}{}
		lock1.Unlock()
		return
	}
	switch metricType {
	case distance.L2:
		computeDistance = distance.L2Impl
	case distance.IP, distance.COSINE:
		if metricType == distance.IP {
			computeDistance = distance.IPImpl
		} else {
			computeDistance = distance.CosineImpl
		}
	default:
		log.Error("unsupported metric type", zap.String("metricType", metricType))
		lock1.Lock()
		neededSegments[segId] = struct{}{}
		lock1.Unlock()
		return
	}

	var dis float32
	for i := 0; i < len(fieldStat.Centroids); i++ {
		dis = computeDistance(vectorFloat, fieldStat.Centroids[i].GetValue().([]float32))
		log.Debug("Segment distance", zap.Int64("segId", segId), zap.Int("i", i), zap.Float32("distance", dis))
		lock2.Lock()
		key := fmt.Sprintf("%d%d", segId, i)
		segmentsToSearch[key] = segmentDisStruct{
			segmentID: segId,
			distance:  dis,
			rows:      numRows,
		}
		lock2.Unlock()
	}
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
		segmentsToSearchMap := make(map[string]segmentDisStruct)
		var wg sync.WaitGroup
		lock1 := &sync.Mutex{}
		lock2 := &sync.Mutex{}
		floatVector := clustering.DeserializeFloatVector(vecBytes)
		for segId, segStats := range partitionStats.SegmentStats {
			// here, we do not skip needed segments required by former query vector
			// meaning that repeated calculation will be carried and the larger the nq is
			// the more segments have to be included and prune effect will decline
			// 1. calculate distances from centroids
			for _, fieldStat := range segStats.FieldStats {
				if fieldStat.FieldID == keyField.GetFieldID() {
					wg.Add(1)
					go DistanceToCentroid(floatVector, fieldStat, keyField, segStats.NumRows, searchReq.GetMetricType(),
						neededSegments, segId, segmentsToSearchMap, lock1, lock2, &wg)
				}
			}
		}
		wg.Wait()
		segmentsToSearch := maps.Values(segmentsToSearchMap)

		// 1. sort segments by geometry score
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

		// 2. determine fixed number of segments to search
		segmentCount := len(segmentsToSearch)
		targetSegNum := int(math.Sqrt(float64(segmentCount)) * filterRatio)
		if targetSegNum <= 0 {
			targetSegNum = 1
		}
		if targetSegNum > segmentCount {
			log.Ctx(context.TODO()).Debug("Warn! targetSegNum is larger or equal than segmentCount, no prune effect at all",
				zap.Int("targetSegNum", targetSegNum),
				zap.Int("segmentCount", segmentCount),
				zap.Float64("filterRatio", filterRatio))
			targetSegNum = segmentCount
		}
		if !gPrunePrinted {
			log.Warn("Pruning segments", zap.Int("targetSegNum", targetSegNum), zap.Int("segmentCount", segmentCount))
			gPrunePrinted = true
		}
		optimizedRowCount := 0
		added := 0
		// forcing the same number of segments to search for e
		for i := 0; i < segmentCount && added < targetSegNum; i++ {
			seg := segmentsToSearch[i]

			// deduplicate by segmentID
			if _, ok := neededSegments[seg.segmentID]; ok {
				continue
			}

			neededSegments[seg.segmentID] = struct{}{}
			optimizedRowCount += seg.rows
			added++
		}

		log.Debug("Needed Segments", zap.Int("count", len(neededSegments)), zap.Any("segments", maps.Keys(neededSegments)))
	}

	// 3. set unnecessary segments as removed
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
			// todo: add float/double/timestmaptz pruner
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

// PruneSealedSegmentsByPKFilter prunes sealedSegments in-place by evaluating the
// PK predicate from serializedExprPlan against each segment's bloom-filter candidate.
// Segments whose candidate data proves they cannot contain a matching PK are removed.
// Workers that end up with no segments are skipped entirely by organizeSubTask.
func PruneSealedSegmentsByPKFilter(
	ctx context.Context,
	serializedExprPlan []byte,
	pkFilter int32,
	sealedSegments []SnapshotItem,
	collectionID int64,
	queryType string,
) {
	if pkFilter == common.PkFilterNoPkFilter {
		return
	}

	plan := &planpb.PlanNode{}
	if err := proto.Unmarshal(serializedExprPlan, plan); err != nil {
		mlog.Warn(ctx, "PruneSealedSegmentsByPKFilter: failed to unmarshal plan, skipping",
			mlog.Err(err))
		return
	}

	expr := BuildPKFilterExpr(plan, pkFilter)
	if expr == nil {
		return
	}

	totalCount := 0
	skippedCount := 0

	for idx, item := range sealedSegments {
		totalCount += len(item.Segments)

		// Collect candidates with valid BF data as PKFilterTargets.
		targets := make([]PKFilterTarget, 0, len(item.Segments))
		for _, entry := range item.Segments {
			if entry.Candidate != nil {
				targets = append(targets, entry.Candidate)
			}
		}

		ids, all := CheckPKFilter(expr, targets)
		if all {
			continue
		}

		newSegs := make([]SegmentEntry, 0, len(item.Segments))
		for _, entry := range item.Segments {
			if entry.Candidate == nil || ids.Contain(entry.SegmentID) {
				newSegs = append(newSegs, entry)
			} else {
				skippedCount++
			}
		}
		item.Segments = newSegs
		sealedSegments[idx] = item
	}

	observePKFilterMetrics(collectionID, queryType, totalCount, skippedCount)
}

func observePKFilterMetrics(collectionID int64, queryType string, totalCount, skippedCount int) {
	nodeID := paramtable.GetStringNodeID()
	collectionIDLabel := fmt.Sprint(collectionID)

	metrics.QueryNodeSegmentFilterTotalSegmentNum.
		WithLabelValues(nodeID, collectionIDLabel, queryType).
		Observe(float64(totalCount))
	metrics.QueryNodeSegmentFilterSkippedSegmentNum.
		WithLabelValues(nodeID, collectionIDLabel, queryType).
		Observe(float64(skippedCount))
	metrics.QueryNodeSegmentFilterHitSegmentNum.
		WithLabelValues(nodeID, collectionIDLabel, queryType).
		Observe(float64(totalCount - skippedCount))
}
