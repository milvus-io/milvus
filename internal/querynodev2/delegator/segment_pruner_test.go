package delegator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
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
	targetPartition     int64
	dim                 int
	sealedSegments      []SnapshotItem
}

func (sps *SegmentPrunerSuite) SetupForClustering(clusterKeyFieldName string) {
	sps.collectionName = "test_segment_prune"
	sps.primaryFieldName = "pk"
	sps.clusterKeyFieldName = clusterKeyFieldName
	sps.dim = 8

	fieldName2DataType := make(map[string]schemapb.DataType)
	fieldName2DataType[sps.primaryFieldName] = schemapb.DataType_Int64
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
	switch fieldName2DataType[sps.clusterKeyFieldName] {
	case schemapb.DataType_Int64:
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
				Min:       storage.NewVarCharFieldValue("ab"),
				Max:       storage.NewVarCharFieldValue("bbc"),
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
				Min:       storage.NewVarCharFieldValue("hhh"),
				Max:       storage.NewVarCharFieldValue("jjx"),
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
				Min:       storage.NewVarCharFieldValue("kkk"),
				Max:       storage.NewVarCharFieldValue("lmn"),
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
				Min:       storage.NewVarCharFieldValue("oo2"),
				Max:       storage.NewVarCharFieldValue("pptt"),
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
	sps.SetupForClustering("age")
	paramtable.Init()
	targetPartitions := make([]UniqueID, 0)
	targetPartitions = append(targetPartitions, sps.targetPartition)
	{
		// test for exact values
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age==156"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for not-equal operator, which is unsupported
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age!=156"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for term operator
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age in [100,200,300]"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for not operator, segment prune don't support not operator
		// so it's expected to get all segments here
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age not in [100,200,300]"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for range one expr part
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=700"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=500 and age<=550"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
	{
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "500<=age<=550"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
	{
		// test for multiple ranges connected with or operator
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "(age>=500 and age<=550) or (age>800 and age<950) or (age>300 and age<330)"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(1, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}

	{
		// test for multiple ranges connected with or operator
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "(age>=500 and age<=550) or (age>800 and age<950) or (age>300 and age<330) or age < 150"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for multiple ranges connected with or operator
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age > 600 or age < 300"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for multiple ranges connected with or operator
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age > 600 or age < 30"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsWithUnrelatedField() {
	sps.SetupForClustering("age")
	paramtable.Init()
	targetPartitions := make([]UniqueID, 0)
	targetPartitions = append(targetPartitions, sps.targetPartition)
	{
		// test for unrelated fields
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=500 and age<=550 and info != 'xxx'"
		// as info is not cluster key field, so 'and' one more info condition will not influence the pruned result
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
	{
		// test for unrelated fields
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=500 and info != 'xxx' and age<=550"
		// as info is not cluster key field, so 'and' one more info condition will not influence the pruned result
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
	{
		// test for unrelated fields
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "age>=500 and age<=550 or info != 'xxx'"
		// as info is not cluster key field, so 'or' one more will make it impossible to prune any segments
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for multiple ranges + unrelated field + or connector
		// as info is not cluster key and or operator is applied, so prune cannot work and have to search all segments in this case
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "(age>=500 and age<=550) or info != 'xxx' or (age>800 and age<950) or (age>300 and age<330) or age < 50"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		// test for multiple ranges + unrelated field + and connector
		// as info is not cluster key and 'and' operator is applied, so prune conditions can work
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "(age>=500 and age<=550) and info != 'xxx' or (age>800 and age<950) or (age>300 and age<330) or age < 50"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       targetPartitions,
		}
		PruneSegments(context.TODO(), sps.partitionStats, nil, queryReq, sps.schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(1, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}

	{
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := "info in ['aa','bb','cc']"
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsByScalarStrField() {
	sps.SetupForClustering("info")
	paramtable.Init()
	targetPartitions := make([]UniqueID, 0)
	targetPartitions = append(targetPartitions, sps.targetPartition)
	{
		// test for exact str values
		testSegments := make([]SnapshotItem, len(sps.sealedSegments))
		copy(testSegments, sps.sealedSegments)
		exprStr := `info=="rag"`
		schemaHelper, _ := typeutil.CreateSchemaHelper(sps.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
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
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableVectorClusteringKey.Key, "true")
	sps.SetupForClustering("vec")
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

func (sps *SegmentPrunerSuite) TestPruneSegmentsVariousIntTypes() {
	paramtable.Init()
	collectionName := "test_segment_prune"
	primaryFieldName := "pk"
	dim := 8
	var targetPartition int64 = 1
	const INT8 = "int8"
	const INT16 = "int16"
	const INT32 = "int32"
	const INT64 = "int64"
	const VEC = "vec"

	fieldName2DataType := make(map[string]schemapb.DataType)
	fieldName2DataType[primaryFieldName] = schemapb.DataType_Int64
	fieldName2DataType[INT8] = schemapb.DataType_Int8
	fieldName2DataType[INT16] = schemapb.DataType_Int16
	fieldName2DataType[INT32] = schemapb.DataType_Int32
	fieldName2DataType[INT64] = schemapb.DataType_Int64
	fieldName2DataType[VEC] = schemapb.DataType_FloatVector

	{
		// test for int8 cluster field
		clusterFieldName := INT8
		schema := testutil.ConstructCollectionSchemaWithKeys(collectionName,
			fieldName2DataType,
			primaryFieldName,
			"",
			clusterFieldName,
			false,
			dim)

		var clusteringKeyFieldID int64 = 0
		for _, field := range schema.GetFields() {
			if field.IsClusteringKey {
				clusteringKeyFieldID = field.FieldID
				break
			}
		}

		// set up part stats
		segStats := make(map[UniqueID]storage.SegmentStats)
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int8,
				Min:     storage.NewInt8FieldValue(-127),
				Max:     storage.NewInt8FieldValue(-23),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}

		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int8,
				Min:     storage.NewInt8FieldValue(-22),
				Max:     storage.NewInt8FieldValue(-8),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int8,
				Min:     storage.NewInt8FieldValue(-7),
				Max:     storage.NewInt8FieldValue(15),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int8,
				Min:     storage.NewInt8FieldValue(16),
				Max:     storage.NewInt8FieldValue(127),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		partitionStats[targetPartition] = &storage.PartitionStatsSnapshot{
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

		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int8 > 128"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(2, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int8 < -129"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(2, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int8 > 50"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(0, len(testSegments[0].Segments))
			sps.Equal(1, len(testSegments[1].Segments))
		}
	}
	{
		// test for int16 cluster field
		clusterFieldName := INT16
		schema := testutil.ConstructCollectionSchemaWithKeys(collectionName,
			fieldName2DataType,
			primaryFieldName,
			"",
			clusterFieldName,
			false,
			dim)

		var clusteringKeyFieldID int64 = 0
		for _, field := range schema.GetFields() {
			if field.IsClusteringKey {
				clusteringKeyFieldID = field.FieldID
				break
			}
		}

		// set up part stats
		segStats := make(map[UniqueID]storage.SegmentStats)
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int16,
				Min:     storage.NewInt16FieldValue(-3127),
				Max:     storage.NewInt16FieldValue(-2123),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}

		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int16,
				Min:     storage.NewInt16FieldValue(-2112),
				Max:     storage.NewInt16FieldValue(-1118),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int16,
				Min:     storage.NewInt16FieldValue(-17),
				Max:     storage.NewInt16FieldValue(3315),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int16,
				Min:     storage.NewInt16FieldValue(3415),
				Max:     storage.NewInt16FieldValue(4127),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		partitionStats[targetPartition] = &storage.PartitionStatsSnapshot{
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

		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int16 > 32768"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(2, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int16 < -32769"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(2, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
		{
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int16 > 2550"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(0, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
	}

	{
		// test for int32 cluster field
		clusterFieldName := INT32
		schema := testutil.ConstructCollectionSchemaWithKeys(collectionName,
			fieldName2DataType,
			primaryFieldName,
			"",
			clusterFieldName,
			false,
			dim)

		var clusteringKeyFieldID int64 = 0
		for _, field := range schema.GetFields() {
			if field.IsClusteringKey {
				clusteringKeyFieldID = field.FieldID
				break
			}
		}

		// set up part stats
		segStats := make(map[UniqueID]storage.SegmentStats)
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int32,
				Min:     storage.NewInt32FieldValue(-13127),
				Max:     storage.NewInt32FieldValue(-12123),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}

		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int32,
				Min:     storage.NewInt32FieldValue(-5127),
				Max:     storage.NewInt32FieldValue(-3123),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int32,
				Min:     storage.NewInt32FieldValue(-3121),
				Max:     storage.NewInt32FieldValue(-1123),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Int32,
				Min:     storage.NewInt32FieldValue(3121),
				Max:     storage.NewInt32FieldValue(41123),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		partitionStats[targetPartition] = &storage.PartitionStatsSnapshot{
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

		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int32 > 2147483648"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(2, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int32 < -2147483649"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(2, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "int32 > 12550"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(0, len(testSegments[0].Segments))
			sps.Equal(1, len(testSegments[1].Segments))
		}
	}
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsFloatTypes() {
	paramtable.Init()
	collectionName := "test_segment_prune"
	primaryFieldName := "pk"
	dim := 8
	var targetPartition int64 = 1
	const FLOAT = "float"
	const DOUBLE = "double"
	const VEC = "vec"

	fieldName2DataType := make(map[string]schemapb.DataType)
	fieldName2DataType[primaryFieldName] = schemapb.DataType_Int64
	fieldName2DataType[FLOAT] = schemapb.DataType_Float
	fieldName2DataType[DOUBLE] = schemapb.DataType_Double
	fieldName2DataType[VEC] = schemapb.DataType_FloatVector

	{
		// test for float cluster field
		clusterFieldName := FLOAT
		schema := testutil.ConstructCollectionSchemaWithKeys(collectionName,
			fieldName2DataType,
			primaryFieldName,
			"",
			clusterFieldName,
			false,
			dim)

		var clusteringKeyFieldID int64 = 0
		for _, field := range schema.GetFields() {
			if field.IsClusteringKey {
				clusteringKeyFieldID = field.FieldID
				break
			}
		}

		// set up part stats
		segStats := make(map[UniqueID]storage.SegmentStats)
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Float,
				Min:     storage.NewFloatFieldValue(-3.0),
				Max:     storage.NewFloatFieldValue(-1.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}

		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Float,
				Min:     storage.NewFloatFieldValue(-0.5),
				Max:     storage.NewFloatFieldValue(2.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Float,
				Min:     storage.NewFloatFieldValue(2.5),
				Max:     storage.NewFloatFieldValue(5.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Float,
				Min:     storage.NewFloatFieldValue(5.5),
				Max:     storage.NewFloatFieldValue(8.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		partitionStats[targetPartition] = &storage.PartitionStatsSnapshot{
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

		{
			// test out bound int expr, fallback to common search
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "float > 3.5"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(0, len(testSegments[0].Segments))
			sps.Equal(2, len(testSegments[1].Segments))
		}
	}

	{
		// test for double cluster field
		clusterFieldName := DOUBLE
		schema := testutil.ConstructCollectionSchemaWithKeys(collectionName,
			fieldName2DataType,
			primaryFieldName,
			"",
			clusterFieldName,
			false,
			dim)

		var clusteringKeyFieldID int64 = 0
		for _, field := range schema.GetFields() {
			if field.IsClusteringKey {
				clusteringKeyFieldID = field.FieldID
				break
			}
		}

		// set up part stats
		segStats := make(map[UniqueID]storage.SegmentStats)
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(-3.0),
				Max:     storage.NewDoubleFieldValue(-1.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}

		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(-0.5),
				Max:     storage.NewDoubleFieldValue(1.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(1.5),
				Max:     storage.NewDoubleFieldValue(3.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(4.0),
				Max:     storage.NewDoubleFieldValue(5.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		partitionStats[targetPartition] = &storage.PartitionStatsSnapshot{
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

		{
			testSegments := make([]SnapshotItem, len(sealedSegments))
			copy(testSegments, sealedSegments)
			exprStr := "double < -1.5"
			schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
			planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			sps.NoError(err)
			serializedPlan, _ := proto.Marshal(planNode)
			queryReq := &internalpb.RetrieveRequest{
				SerializedExprPlan: serializedPlan,
			}
			PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
			sps.Equal(1, len(testSegments[0].Segments))
			sps.Equal(0, len(testSegments[1].Segments))
		}
	}
}

func (sps *SegmentPrunerSuite) TestPruneSegmentsWithoutPartitionStats() {
	paramtable.Init()
	collectionName := "test_segment_prune"
	primaryFieldName := "pk"
	dim := 8
	const FLOAT = "float"
	const DOUBLE = "double"
	const VEC = "vec"

	fieldName2DataType := make(map[string]schemapb.DataType)
	fieldName2DataType[primaryFieldName] = schemapb.DataType_Int64
	fieldName2DataType[FLOAT] = schemapb.DataType_Float
	fieldName2DataType[DOUBLE] = schemapb.DataType_Double
	fieldName2DataType[VEC] = schemapb.DataType_FloatVector

	// set up segment distribution
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

	clusterFieldName := DOUBLE
	schema := testutil.ConstructCollectionSchemaWithKeys(collectionName,
		fieldName2DataType,
		primaryFieldName,
		"",
		clusterFieldName,
		false,
		dim)
	{
		// test for empty partition stats
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		testSegments := make([]SnapshotItem, len(sealedSegments))
		copy(testSegments, sealedSegments)
		exprStr := "double < -1.5"
		schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       []int64{1},
		}
		PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		// without valid partition stats, we should get all segments targeted
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}
	{
		// test for nil partition stat
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		partitionStats[1] = nil
		testSegments := make([]SnapshotItem, len(sealedSegments))
		copy(testSegments, sealedSegments)
		exprStr := "double < -1.5"
		schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       []int64{1},
		}
		PruneSegments(context.TODO(), partitionStats, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		// without valid partition stats, we should get all segments targeted
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}
	{
		// test for nil partition stats map
		testSegments := make([]SnapshotItem, len(sealedSegments))
		copy(testSegments, sealedSegments)
		exprStr := "double < -1.5"
		schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       []int64{1},
		}
		PruneSegments(context.TODO(), nil, nil, queryReq, schema, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		// without valid partition stats, we should get all segments targeted
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}
	{
		// test for nil schema
		var clusteringKeyFieldID int64 = 0
		for _, field := range schema.GetFields() {
			if field.IsClusteringKey {
				clusteringKeyFieldID = field.FieldID
				break
			}
		}

		// set up part stats
		segStats := make(map[UniqueID]storage.SegmentStats)
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(-5.0),
				Max:     storage.NewDoubleFieldValue(-2.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[1] = *storage.NewSegmentStats(fieldStats, 80)
		}

		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(-1.0),
				Max:     storage.NewDoubleFieldValue(2.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[2] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(3.0),
				Max:     storage.NewDoubleFieldValue(6.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[3] = *storage.NewSegmentStats(fieldStats, 80)
		}
		{
			fieldStats := make([]storage.FieldStats, 0)
			fieldStat1 := storage.FieldStats{
				FieldID: clusteringKeyFieldID,
				Type:    schemapb.DataType_Double,
				Min:     storage.NewDoubleFieldValue(7.0),
				Max:     storage.NewDoubleFieldValue(8.0),
			}
			fieldStats = append(fieldStats, fieldStat1)
			segStats[4] = *storage.NewSegmentStats(fieldStats, 80)
		}
		partitionStats := make(map[UniqueID]*storage.PartitionStatsSnapshot)
		targetPartition := int64(1)
		partitionStats[targetPartition] = &storage.PartitionStatsSnapshot{
			SegmentStats: segStats,
		}
		testSegments := make([]SnapshotItem, len(sealedSegments))
		copy(testSegments, sealedSegments)
		exprStr := "double < -1.5"
		schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		sps.NoError(err)
		serializedPlan, _ := proto.Marshal(planNode)
		queryReq := &internalpb.RetrieveRequest{
			SerializedExprPlan: serializedPlan,
			PartitionIDs:       []int64{targetPartition},
		}
		PruneSegments(context.TODO(), partitionStats, nil, queryReq, nil, testSegments, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		sps.Equal(2, len(testSegments[0].Segments))
		sps.Equal(2, len(testSegments[1].Segments))
	}
}

func TestSegmentPrunerSuite(t *testing.T) {
	suite.Run(t, new(SegmentPrunerSuite))
}
