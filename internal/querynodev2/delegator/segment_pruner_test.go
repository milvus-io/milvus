package delegator

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SegmentPrunerSuite struct {
	suite.Suite
	partitionStats      map[UniqueID]*storage.PartitionStatsSnapshot
	schema              *schemapb.CollectionSchema
	collectionName      string
	primaryFieldName    string
	clusterKeyFieldName string
	autoID              bool
	targetPartition     int64
	dim                 int
	sealedSegments      []SnapshotItem
}

func (sps *SegmentPrunerSuite) SetupForClustering(clusterKeyFieldName string,
	clusterKeyFieldType schemapb.DataType,
) {
	sps.collectionName = "test_segment_prune"
	sps.primaryFieldName = "pk"
	sps.clusterKeyFieldName = clusterKeyFieldName
	sps.autoID = true
	sps.dim = 8

	fieldName2DataType := make(map[string]schemapb.DataType)
	fieldName2DataType[sps.primaryFieldName] = schemapb.DataType_Int64
	fieldName2DataType[sps.clusterKeyFieldName] = clusterKeyFieldType
	fieldName2DataType["info"] = schemapb.DataType_VarChar
	fieldName2DataType["age"] = schemapb.DataType_Int64
	fieldName2DataType["vec"] = schemapb.DataType_FloatVector

	sps.schema = testutil.ConstructCollectionSchemaWithKeys(sps.collectionName,
		fieldName2DataType,
		sps.primaryFieldName,
		"",
		sps.clusterKeyFieldName,
		false,
		sps.dim)

	var clusteringKeyFieldID int64 = 0
	for _, field := range sps.schema.GetFields() {
		if field.IsClusteringKey {
			clusteringKeyFieldID = field.FieldID
			break
		}
	}
	centroids1 := []storage.VectorFieldValue{
		&storage.FloatVectorFieldValue{
			Value: []float32{0.6951474, 0.45225978, 0.51508516, 0.24968886, 0.6085484, 0.964968, 0.32239532, 0.7771577},
		},
	}
	centroids2 := []storage.VectorFieldValue{
		&storage.FloatVectorFieldValue{
			Value: []float32{0.12345678, 0.23456789, 0.34567890, 0.45678901, 0.56789012, 0.67890123, 0.78901234, 0.89012345},
		},
	}
	centroids3 := []storage.VectorFieldValue{
		&storage.FloatVectorFieldValue{
			Value: []float32{0.98765432, 0.87654321, 0.76543210, 0.65432109, 0.54321098, 0.43210987, 0.32109876, 0.21098765},
		},
	}
	centroids4 := []storage.VectorFieldValue{
		&storage.FloatVectorFieldValue{
			Value: []float32{0.11111111, 0.22222222, 0.33333333, 0.44444444, 0.55555555, 0.66666666, 0.77777777, 0.88888888},
		},
	}

	// init partition stats
	// here, for convenience, we set up both min/max and Centroids
	// into the same struct, in the real user cases, a field stat
	// can either contain min&&max or centroids
	segStats := make(map[UniqueID]storage.SegmentStats)
	switch clusterKeyFieldType {
	case schemapb.DataType_Int64, schemapb.DataType_Int32, schemapb.DataType_Int16, schemapb.DataType_Int8:
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_Int64,
				Min:       storage.NewInt64FieldValue(100),
				Max:       storage.NewInt64FieldValue(200),
				Centroids: centroids1,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_Int64,
				Min:       storage.NewInt64FieldValue(100),
				Max:       storage.NewInt64FieldValue(400),
				Centroids: centroids2,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_Int64,
				Min:       storage.NewInt64FieldValue(600),
				Max:       storage.NewInt64FieldValue(900),
				Centroids: centroids3,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_Int64,
				Min:       storage.NewInt64FieldValue(500),
				Max:       storage.NewInt64FieldValue(1000),
				Centroids: centroids4,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
	default:
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_VarChar,
				Min:       storage.NewStringFieldValue("ab"),
				Max:       storage.NewStringFieldValue("bbc"),
				Centroids: centroids1,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_VarChar,
				Min:       storage.NewStringFieldValue("hhh"),
				Max:       storage.NewStringFieldValue("jjx"),
				Centroids: centroids2,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_VarChar,
				Min:       storage.NewStringFieldValue("kkk"),
				Max:       storage.NewStringFieldValue("lmn"),
				Centroids: centroids3,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID:   clusteringKeyFieldID,
				Type:      schemapb.DataType_VarChar,
				Min:       storage.NewStringFieldValue("oo2"),
				Max:       storage.NewStringFieldValue("pptt"),
				Centroids: centroids4,
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
	}
	sps.partitionStats = make(map[UniqueID]*storage.PartitionStatsSnapshot)
	sps.targetPartition = 11111
	sps.partitionStats[sps.targetPartition] = &storage.PartitionStatsSnapshot{
		SegmentStats: segStats,
	}

	sealedSegments := make([]SnapshotItem, 0)
	item1 := SnapshotItem{
		NodeID: 1,
		Segments: []SegmentEntry{
			{
				NodeID:    1,
				SegmentID: 1,
			},
			{
				NodeID:    1,
				SegmentID: 2,
			},
		},
	}
	item2 := SnapshotItem{
		NodeID: 2,
		Segments: []SegmentEntry{
			{
				NodeID:    2,
				SegmentID: 3,
			},
			{
				NodeID:    2,
				SegmentID: 4,
			},
		},
	}
	sealedSegments = append(sealedSegments, item1)
	sealedSegments = append(sealedSegments, item2)
	sps.sealedSegments = sealedSegments
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsByScalarIntField() {
	sps.SetupForClustering("age", schemapb.DataType_Int32)
	paramtable.Init()
	targetPartitions := make([]UniqueID, 0)
	targetPartitions = append(targetPartitions, sps.targetPartition)
	{
		// test for exact values
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age==156"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(0, len(testSegments[1].Segments))
	}
	{
		// test for range one expr part
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=700"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(0, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}
	{
		// test for unlogical binary range
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=700 and age<=500"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}
	{
		// test for unlogical binary range
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=500 and age<=550"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(0, len(testSegments[0].Segments))
		sps.Equal(1, len(testSegments[1].Segments))
	}
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsByScalarStrField() {
	sps.SetupForClustering("info", schemapb.DataType_VarChar)
	paramtable.Init()
	targetPartitions := make([]UniqueID, 0)
	targetPartitions = append(targetPartitions, sps.targetPartition)
	{
		// test for exact str values
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := `info=="rag"`
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(0, len(testSegments[0].Segments))
		sps.Equal(0, len(testSegments[1].Segments))
		// there should be no segments fulfilling the info=="rag"
	}
	{
		// test for exact str values
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := `info=="kpl"`
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(0, len(testSegments[0].Segments))
		sps.Equal(1, len(testSegments[1].Segments))
		// there should be no segments fulfilling the info=="rag"
	}
	{
		// test for unary  str values
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := `info<="less"`
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(1, len(testSegments[1].Segments))
		// there should be no segments fulfilling the info=="rag"
	}
}

func vector2Placeholder(vectors [][]float32) *commonpb.PlaceholderValue {
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) == 0 {
		return ph
	}

	ph.Type = commonpb.PlaceholderType_FloatVector
	for _, vector := range vectors {
		ph.Values = append(ph.Values, clustering.SerializeFloatVector(vector))
	}
	return ph
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsByVectorField() {
	paramtable.Init()
	sps.SetupForClustering("vec", schemapb.DataType_FloatVector)
	vector1 := []float32{0.8877872002188053, 0.6131822285635065, 0.8476814632326242, 0.6645877829359371, 0.9962627712600025, 0.8976183052440327, 0.41941169325798844, 0.7554387854258499}
	vector2 := []float32{0.8644394874390322, 0.023327886647378615, 0.08330118483461302, 0.7068040179963112, 0.6983994910799851, 0.5562075958994153, 0.3288536247938002, 0.07077341010237759}
	vectors := [][]float32{vector1, vector2}

	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			vector2Placeholder(vectors),
		},
	}
	bs, _ := proto.Marshal(phg)
	// test for L2 metrics
	req := &internalpb.SearchRequest{
		MetricType:       "L2",
		PlaceholderGroup: bs,
		PartitionIDs:     []UniqueID{sps.targetPartition},
		Topk:             100,
	}

	PruneSegments(context.TODO(), sps.partitionStats, req, nil, sps.schema, sps.sealedSegments, PruneInfo{1})
	sps.Equal(1, len(sps.sealedSegments[0].Segments))
	sps.Equal(int64(1), sps.sealedSegments[0].Segments[0].SegmentID)
	sps.Equal(1, len(sps.sealedSegments[1].Segments))
	sps.Equal(int64(3), sps.sealedSegments[1].Segments[0].SegmentID)
}

func TestSegmentPrunerSuite(t *testing.T) {
	suite.Run(t, new(SegmentPrunerSuite))
}
