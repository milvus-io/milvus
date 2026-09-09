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

package querynodev2

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGetPipelineJSON(t *testing.T) {
	paramtable.Init()

	ch := "ch"
	delegators := typeutil.NewConcurrentMap[string, delegator.ShardDelegator]()
	d := delegator.NewMockShardDelegator(t)
	d.EXPECT().GetTSafe().Return(0)
	delegators.Insert(ch, d)
	msgDispatcher := msgdispatcher.NewMockClient(t)

	collectionManager := segments.NewMockCollectionManager(t)
	segmentManager := segments.NewMockSegmentManager(t)
	collectionManager.EXPECT().Get(mock.Anything).Return(segments.NewTestCollection(1, querypb.LoadType_UnKnownType, &schemapb.CollectionSchema{}))
	manager := &segments.Manager{
		Collection: collectionManager,
		Segment:    segmentManager,
	}

	pipelineManager := pipeline.NewManager(manager, msgDispatcher, delegators)

	_, err := pipelineManager.Add(1, ch)
	assert.NoError(t, err)
	assert.Equal(t, 1, pipelineManager.Num())

	stats := pipelineManager.GetChannelStats(0)
	expectedStats := []*metricsinfo.Channel{
		{
			Name:           ch,
			WatchState:     "Healthy",
			LatestTimeTick: tsoutil.PhysicalTimeFormat(0),
			NodeID:         paramtable.GetNodeID(),
			CollectionID:   1,
		},
	}
	assert.Equal(t, expectedStats, stats)

	JSONStr := getChannelJSON(&QueryNode{pipelineManager: pipelineManager}, 0)
	assert.NotEmpty(t, JSONStr)

	var actualStats []*metricsinfo.Channel
	err = json.Unmarshal([]byte(JSONStr), &actualStats)
	assert.NoError(t, err)
	assert.Equal(t, expectedStats, actualStats)
}

func TestGetSegmentJSON(t *testing.T) {
	segment := segments.NewMockSegment(t)
	segment.EXPECT().ID().Return(int64(1))
	segment.EXPECT().Collection().Return(int64(1001))
	segment.EXPECT().Partition().Return(int64(2001))
	segment.EXPECT().MemSize().Return(int64(1024))
	segment.EXPECT().HasRawData(mock.Anything).Return(true)
	segment.EXPECT().Indexes().Return([]*segments.IndexedFieldInfo{
		{
			IndexInfo: &querypb.FieldIndexInfo{
				FieldID:   1,
				IndexID:   101,
				IndexSize: 512,
				BuildID:   10001,
			},
			IsLoaded: true,
		},
	})
	segment.EXPECT().Type().Return(segments.SegmentTypeGrowing)
	segment.EXPECT().ResourceGroup().Return("default")
	segment.EXPECT().InsertCount().Return(int64(100))

	node := &QueryNode{}
	mockedSegmentManager := segments.NewMockSegmentManager(t)
	mockedSegmentManager.EXPECT().GetBy().Return([]segments.Segment{segment})
	node.manager = &segments.Manager{Segment: mockedSegmentManager}

	jsonStr := getSegmentJSON(node, 0)
	assert.NotEmpty(t, jsonStr)

	var segments []*metricsinfo.Segment
	err := json.Unmarshal([]byte(jsonStr), &segments)
	assert.NoError(t, err)
	assert.NotNil(t, segments)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[0].SegmentID)
	assert.Equal(t, int64(1001), segments[0].CollectionID)
	assert.Equal(t, int64(2001), segments[0].PartitionID)
	assert.Equal(t, int64(1024), segments[0].MemSize)
	assert.Equal(t, 1, len(segments[0].IndexedFields))
	assert.Equal(t, int64(1), segments[0].IndexedFields[0].IndexFieldID)
	assert.Equal(t, int64(101), segments[0].IndexedFields[0].IndexID)
	assert.Equal(t, int64(512), segments[0].IndexedFields[0].IndexSize)
	assert.Equal(t, int64(10001), segments[0].IndexedFields[0].BuildID)
	assert.True(t, segments[0].IndexedFields[0].IsLoaded)
	assert.Equal(t, "Growing", segments[0].State)
	assert.Equal(t, "default", segments[0].ResourceGroup)
	assert.Equal(t, int64(100), segments[0].LoadedInsertRowCount)
}

func TestStreamingQuotaMetrics(t *testing.T) {
	paramtable.Init()

	wal := mock_streaming.NewMockWALAccesser(t)
	local := mock_streaming.NewMockLocal(t)
	now := time.Now()
	local.EXPECT().GetMetricsIfLocal(mock.Anything).Return(&types.StreamingNodeMetrics{
		WALMetrics: map[types.ChannelID]types.WALMetrics{
			{Name: "ch1"}: types.RWWALMetrics{
				ChannelInfo: types.PChannelInfo{
					Name: "ch1",
				},
				MVCCTimeTick:     tsoutil.ComposeTSByTime(now),
				RecoveryTimeTick: tsoutil.ComposeTSByTime(now.Add(-time.Second)),
			},
			{Name: "ch2"}: types.ROWALMetrics{},
		},
	}, nil)
	wal.EXPECT().Local().Return(local)
	streaming.SetWALForTest(wal)
	defer streaming.RecoverWALForTest()

	m := getStreamingQuotaMetrics()
	assert.Len(t, m.WALs, 1)
	assert.Equal(t, "ch1", m.WALs[0].Channel.Name)
	assert.Equal(t, tsoutil.ComposeTSByTime(now.Add(-time.Second)), m.WALs[0].RecoveryTimeTick)

	local.EXPECT().GetMetricsIfLocal(mock.Anything).Unset()
	local.EXPECT().GetMetricsIfLocal(mock.Anything).Return(nil, errors.New("test"))
	m = getStreamingQuotaMetrics()
	assert.Nil(t, m)
}

func TestAppendQueryNodeCollectionMemoryUsage(t *testing.T) {
	const nodeID = "101"
	const source = "internal_cache_shard_memory_usage_bytes"
	const growing = "internal_growing_segment_memory_usage_bytes"
	const target = "milvus_querynode_collection_memory_usage_bytes"
	const header = "# TYPE " + source + " gauge\n"
	tests := []struct {
		name     string
		text     string
		expected map[string]float64
	}{
		{
			name: "sum data types and shards by collection",
			text: header +
				source + "{data_type=\"scalar_field\",shard=\"channel_1001v0\"} 128\n" +
				source + "{data_type=\"vector_index\",shard=\"channel_1001v0\"} 256\n" +
				source + "{data_type=\"vector_field\",shard=\"channel_1001v1\"} 512\n" +
				source + "{data_type=\"other\",shard=\"channel_1002v0\"} 32\n" +
				"# TYPE internal_cache_shard_disk_usage_bytes gauge\n" +
				"internal_cache_shard_disk_usage_bytes{shard=\"channel_1001v0\"} 4096\n",
			expected: map[string]float64{"1001": 896, "1002": 32},
		},
		{
			name:     "single shard",
			text:     header + source + "{shard=\"channel_1001v0\"} 64\n",
			expected: map[string]float64{"1001": 64},
		},
		{
			name: "sum sealed and growing memory",
			text: header + source + "{shard=\"channel_1001v0\"} 128\n" +
				"# TYPE " + growing + " gauge\n" +
				growing + "{collection_id=\"1001\"} 512\n" +
				growing + "{collection_id=\"1002\"} 32\n",
			expected: map[string]float64{"1001": 640, "1002": 32},
		},
		{
			name: "growing without sealed memory",
			text: "# TYPE " + growing + " gauge\n" +
				growing + "{collection_id=\"1001\"} 64\n" +
				growing + "{collection_id=\"0\"} 128\n" +
				growing + "{collection_id=\"invalid\"} 256\n",
			expected: map[string]float64{"1001": 64},
		},
		{
			name: "skip unknown attribution and retain attributed zero",
			text: header +
				source + "{shard=\"invalid\"} 1\n" +
				source + "{shard=\"channel_0v0\"} 2\n" +
				source + "{shard=\"channel_-1v0\"} 4\n" +
				source + "{shard=\"channel_9223372036854775808v0\"} 8\n" +
				source + "{collection_id=\"1001\"} 16\n" +
				source + "{shard=\"channel_1001v0\"} 0\n",
			expected: map[string]float64{"1001": 0},
		},
		{name: "no source samples"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var parser expfmt.TextParser
			families, err := parser.TextToMetricFamilies(strings.NewReader(test.text))
			require.NoError(t, err)
			original := make(map[string]*dto.MetricFamily, len(families))
			for name, family := range families {
				original[name] = proto.Clone(family).(*dto.MetricFamily)
			}
			appendQueryNodeCollectionMemoryUsage(nodeID, families)
			if len(test.expected) == 0 {
				require.NotContains(t, families, target)
			} else {
				family := families[target]
				require.NotNil(t, family)
				require.Equal(t, dto.MetricType_GAUGE, family.GetType())
				require.Len(t, family.GetMetric(), len(test.expected))
				actual := make(map[string]float64)
				for _, metric := range family.GetMetric() {
					require.Len(t, metric.GetLabel(), 2)
					require.Equal(t, "collection_id", metric.Label[0].GetName())
					require.Equal(t, "node_id", metric.Label[1].GetName())
					require.Equal(t, nodeID, metric.Label[1].GetValue())
					actual[metric.Label[0].GetValue()] = metric.GetGauge().GetValue()
				}
				require.Equal(t, test.expected, actual)
			}
			for name, family := range original {
				require.True(t, proto.Equal(family, families[name]), name)
			}
		})
	}
}
