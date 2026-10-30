package pruning

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NoPruningFlags indicates that no candidates were excluded by statistics.
const NoPruningFlags uint32 = 0

// Exclude returns only exclusions from this View's actual candidates. Unknown
// statistics/expressions retain candidates. Statistics are immutable and borrowed.
// The returned flags describe actual exclusions using the already-resolved field.
func Exclude(ctx context.Context, schema *schemapb.CollectionSchema, stats map[int64]*storage.PartitionStatsSnapshot, candidates map[int64][]int64, search *internalpb.SearchRequest, retrieve *internalpb.RetrieveRequest, ratio float64) (map[int64]struct{}, uint32) {
	excluded := make(map[int64]struct{})
	field := clustering.GetClusteringKeyField(schema)
	if field == nil || len(stats) == 0 {
		return excluded, NoPruningFlags
	}
	vector := typeutil.IsVectorType(field.GetDataType())
	var predicate Expr
	var vectors [][]byte
	var dim int64
	if vector {
		// A retrieve or a search on a different vector field has no matching
		// centroid-distance predicate. Never interpret another field's vectors.
		if search == nil || search.GetFieldId() != field.GetFieldID() || field.GetDataType() != schemapb.DataType_FloatVector {
			return excluded, NoPruningFlags
		}
		if search.GetMetricType() != metric.L2 && search.GetMetricType() != metric.IP && search.GetMetricType() != metric.COSINE {
			return excluded, NoPruningFlags
		}
		holder := &commonpb.PlaceholderGroup{}
		if proto.Unmarshal(search.GetPlaceholderGroup(), holder) != nil || len(holder.GetPlaceholders()) != 1 {
			return excluded, NoPruningFlags
		}
		value := holder.GetPlaceholders()[0]
		if value.GetType() != commonpb.PlaceholderType_FloatVector || len(value.GetValues()) == 0 {
			return excluded, NoPruningFlags
		}
		dimText, err := funcutil.GetAttrByKeyFromRepeatedKV(common.DimKey, field.GetTypeParams())
		if err != nil {
			return excluded, NoPruningFlags
		}
		dim, err = strconv.ParseInt(dimText, 10, 64)
		if err != nil || dim <= 0 {
			return excluded, NoPruningFlags
		}
		vectors = value.GetValues()
		for _, vec := range vectors {
			if int64(len(vec)) != dim*4 {
				return excluded, NoPruningFlags
			}
		}
	} else {
		data := retrieve.GetSerializedExprPlan()
		if search != nil {
			data = search.GetSerializedExprPlan()
		}
		plan := &planpb.PlanNode{}
		if proto.Unmarshal(data, plan) != nil {
			return excluded, NoPruningFlags
		}
		expr, err := exprutil.ParseExprFromPlan(plan)
		if err != nil {
			return excluded, NoPruningFlags
		}
		predicate, err = ParseExpr(expr, NewParseContext(field.GetFieldID(), field.GetDataType()))
		if err != nil || predicate == nil {
			return excluded, NoPruningFlags
		}
	}
	for partition, ids := range candidates {
		source := stats[partition]
		if source == nil {
			continue
		}
		selected := &storage.PartitionStatsSnapshot{SegmentStats: make(map[int64]storage.SegmentStats)}
		scalarIDs := make([]int64, 0, len(ids))
		scalarStats := make([]storage.SegmentStats, 0, len(ids))
		for _, id := range ids {
			entry, ok := source.SegmentStats[id]
			if !ok {
				continue
			}
			for _, fs := range entry.FieldStats {
				if fs.FieldID != field.GetFieldID() || fs.Type != field.GetDataType() {
					continue
				}
				if !vector && (fs.Min == nil || fs.Max == nil) {
					break
				}
				// Scalar expressions operate on the selected clustering field,
				// not whichever field happened to appear first in the file.
				compatible := storage.SegmentStats{FieldStats: []storage.FieldStats{fs}, NumRows: entry.NumRows}
				selected.SegmentStats[id] = compatible
				scalarIDs = append(scalarIDs, id)
				scalarStats = append(scalarStats, compatible)
				break
			}
		}
		if vector {
			FilterSegmentsByVector(ctx, selected, search, vectors, dim, field, excluded, ratio)
		} else {
			PruneByScalarField(predicate, scalarStats, scalarIDs, excluded)
		}
	}
	flags := NoPruningFlags
	if len(excluded) > 0 {
		offset := viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering
		if vector {
			offset = viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering
		}
		flags = 1 << offset
	}
	return excluded, flags
}
