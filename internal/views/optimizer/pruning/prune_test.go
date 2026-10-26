package pruning

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestVectorPruningUsesPartitionCandidatesAndMatchingField(t *testing.T) {
	paramtable.Init()
	cfg := paramtable.Get()
	require.NoError(t, cfg.Save(cfg.CommonCfg.EnableVectorClusteringKey.Key, "true"))
	defer cfg.Reset(cfg.CommonCfg.EnableVectorClusteringKey.Key)
	field := &schemapb.FieldSchema{FieldID: 101, IsClusteringKey: true, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	stats := map[int64]*storage.PartitionStatsSnapshot{2: {SegmentStats: map[int64]storage.SegmentStats{}}}
	for id, value := range map[int64]float32{10: 0, 11: 100, 99: 0} {
		stats[2].SegmentStats[id] = storage.SegmentStats{NumRows: 100, FieldStats: []storage.FieldStats{{FieldID: 101, Type: schemapb.DataType_FloatVector, Centroids: []storage.VectorFieldValue{&storage.FloatVectorFieldValue{Value: []float32{value, value}}}}}}
	}
	placeholder := func(values ...float32) []byte {
		vectors := make([][]byte, 0, len(values))
		for _, value := range values {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint32(b, math.Float32bits(value))
			binary.LittleEndian.PutUint32(b[4:], math.Float32bits(value))
			vectors = append(vectors, b)
		}
		result, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_FloatVector, Values: vectors}}})
		require.NoError(t, err)
		return result
	}
	req := &internalpb.SearchRequest{FieldId: 101, MetricType: "L2", Topk: 1, PlaceholderGroup: placeholder(0)}
	candidates := map[int64][]int64{2: {10, 11, 12}}
	excluded, flags := Exclude(t.Context(), schema, stats, candidates, req, nil, 0.5)
	require.Equal(t, map[int64]struct{}{11: {}}, excluded) // unknown 12 retained; stale 99 not budgeted
	require.Equal(t, uint32(1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering), flags)
	req.PlaceholderGroup = placeholder(0, 100)
	excluded, flags = Exclude(t.Context(), schema, stats, candidates, req, nil, 0.5)
	require.Empty(t, excluded)
	require.Equal(t, NoPruningFlags, flags)
	req.FieldId = 102
	excluded, flags = Exclude(t.Context(), schema, stats, candidates, req, nil, 0.5)
	require.Empty(t, excluded)
	require.Equal(t, NoPruningFlags, flags)
	excluded, flags = Exclude(t.Context(), schema, stats, candidates, nil, &internalpb.RetrieveRequest{}, 0.5)
	require.Empty(t, excluded)
	require.Equal(t, NoPruningFlags, flags)
}
