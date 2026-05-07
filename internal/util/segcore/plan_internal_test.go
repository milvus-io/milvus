package segcore

import (
	"math"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

func TestNewSearchRequestSkipsPlaceholderCleanupOnMetricMismatch(t *testing.T) {
	paramtable.Init()

	const dim = 128
	schema := &schemapb.CollectionSchema{
		Name:   "plan-internal-suite",
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
				AutoID:       false,
			},
			{
				FieldID:  101,
				Name:     "fakevec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
	indexMeta := &segcorepb.CollectionIndexMeta{
		IndexMetas: []*segcorepb.FieldIndexMeta{
			{
				CollectionID: 100,
				FieldID:      101,
				IndexName:    "fakevec",
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "L2"},
				},
			},
		},
	}
	collection, err := CreateCCollection(&CreateCCollectionRequest{
		CollectionID: 100,
		Schema:       schema,
		IndexMeta:    indexMeta,
	})
	require.NoError(t, err)
	defer collection.Release()

	vec := testutils.GenerateFloatVectors(1, dim)
	var searchRawData []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData = append(searchRawData, buf...)
	}

	placeholderValue := commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: [][]byte{searchRawData},
	}
	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{&placeholderValue},
	}
	placeholderGroupBytes, err := proto.Marshal(&placeholderGroup)
	require.NoError(t, err)

	planStr := `vector_anns: <
                 field_id: 101
                 query_info: <
                   topk: 10
                   round_decimal: 6
                   metric_type: "L2"
                   search_params: "{\"nprobe\": 10}"
                 >
                 placeholder_tag: "$0"
               >`
	var planNode planpb.PlanNode
	err = prototext.Unmarshal([]byte(planStr), &planNode)
	require.NoError(t, err)

	serializedPlan, err := proto.Marshal(&planNode)
	require.NoError(t, err)

	mockey.PatchConvey("metric mismatch happens before placeholder parsing", t, func() {
		var cleaned atomic.Int32
		mockey.Mock(deletePlaceholderGroup).To(func(_ unsafe.Pointer) {
			cleaned.Add(1)
		}).Build()

		_, err = NewSearchRequest(collection, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: serializedPlan,
				MetricType:         "IP",
			},
		}, placeholderGroupBytes)

		require.Error(t, err)
		require.Contains(t, err.Error(), "metric type not match")
		require.Zero(t, cleaned.Load())
	})
}
