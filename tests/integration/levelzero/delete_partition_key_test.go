package levelzero

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *LevelZeroSuite) TestDeletePartitionKeyHint() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	const (
		indexType  = integration.IndexFaissIvfFlat
		metricType = metric.L2
		vecType    = schemapb.DataType_FloatVector
	)

	collectionName := "TestLevelZero_" + funcutil.GenRandomStr()

	// create a collection with partition key field "partition_key"
	s.schema = integration.ConstructSchema(collectionName, s.dim, false)
	s.schema.Fields = append(s.schema.Fields, &schemapb.FieldSchema{
		FieldID:        102,
		Name:           "partition_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	})

	req := s.buildCreateCollectionRequest(collectionName, s.schema, 2)
	s.createCollection(req)
	c := s.Cluster

	// create index and load
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, indexType, metricType),
	})
	err = merr.CheckRPCCall(createIndexStatus, err)
	s.NoError(err)
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.Require().NoError(err)
	s.WaitForLoad(ctx, collectionName)

	// Generate 2 growing segments with 2 differenct partition key 0, 1001, with exactlly same PK start from 0
	s.generateSegment(collectionName, 1000, 0, false, 0)
	s.generateSegment(collectionName, 1001, 0, false, 1001)
	segments, err := s.Cluster.MetaWatcher.ShowSegments()
	s.Require().NoError(err)
	s.Require().EqualValues(len(segments), 2)
	for _, segment := range segments {
		s.Require().EqualValues(commonpb.SegmentState_Growing, segment.GetState())
		s.Require().EqualValues(commonpb.SegmentLevel_L1, segment.GetLevel())
	}

	L1SegIDs := lo.Map(segments, func(seg *datapb.SegmentInfo, _ int) int64 {
		return seg.GetID()
	})
	L1SegIDSet := typeutil.NewUniqueSet(L1SegIDs...)

	checkRowCount := func(rowCount int) {
		// query
		queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: collectionName,
			OutputFields:   []string{"count(*)"},
		})
		err = merr.CheckRPCCall(queryResult, err)
		s.NoError(err)
		s.EqualValues(rowCount, queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	}
	checkRowCount(2001)

	// delete all data belongs to partition_key == 1001
	// expr: partition_key == 1001 && pk >= 0
	//  - for previous implementation, the delete pk >= 0 will touch every segments and leave only 1 numRows
	//  - for latest enhancements, the expr "pk >= 0" will only touch partitions that contains partition key == 1001
	deleteResult, err := c.Proxy.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("partition_key == 1001 && %s >= 0", integration.Int64Field),
	})
	err = merr.CheckRPCCall(deleteResult, err)
	s.NoError(err)

	checkRowCount(1000)

	// Flush will generates 2 Flushed L1 segments and 1 Flushed L0 segment
	s.Flush(collectionName)

	segments, err = s.Cluster.MetaWatcher.ShowSegments()
	s.Require().NoError(err)
	s.Require().EqualValues(len(segments), 3)
	for _, segment := range segments {
		s.Require().EqualValues(commonpb.SegmentState_Flushed, segment.GetState())
		// L1 segments
		if L1SegIDSet.Contain(segment.GetID()) {
			s.Require().EqualValues(commonpb.SegmentLevel_L1, segment.GetLevel())
		} else { // L0 segment with 1001 delete entries count
			s.Require().EqualValues(commonpb.SegmentLevel_L0, segment.GetLevel())
			s.EqualValues(1001, segment.Deltalogs[0].GetBinlogs()[0].GetEntriesNum())
		}
	}

	l0Dropped := func() bool {
		segments, err := s.Cluster.MetaWatcher.ShowSegments()
		s.Require().NoError(err)
		s.Require().EqualValues(len(segments), 3)

		for _, segment := range segments {
			// Return if L0 segments not compacted
			if !L1SegIDSet.Contain(segment.GetID()) && segment.GetState() == commonpb.SegmentState_Flushed {
				return false
			}

			// If L0 segment compacted
			if !L1SegIDSet.Contain(segment.GetID()) && segment.GetState() == commonpb.SegmentState_Dropped {
				// find the segment belong to partition_key == 1001
				// check for the deltalog entries count == 1001
				if segment.GetLevel() == datapb.SegmentLevel_L1 && segment.GetNumOfRows() == 1001 {
					s.True(L1SegIDSet.Contain(segment.GetID()))
					s.EqualValues(1001, segment.Deltalogs[0].GetBinlogs()[0].GetEntriesNum())
				}

				// find segment of another partition_key == 0
				// check compaction doesn't touch it even though delete expression will delete it all
				if segment.GetLevel() == datapb.SegmentLevel_L1 && segment.GetNumOfRows() == 1000 {
					s.True(L1SegIDSet.Contain(segment.GetID()))
					s.Empty(segment.Deltalogs)
				}

				return true
			}
		}
		return false
	}

	checkL0CompactionTouchOnePartition := func() {
		failT := time.NewTimer(3 * time.Minute)
		checkT := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-failT.C:
				s.FailNow("L0 compaction timeout")
			case <-checkT.C:
				if l0Dropped() {
					failT.Stop()
					return
				}
			}
		}
	}

	checkL0CompactionTouchOnePartition()
}
