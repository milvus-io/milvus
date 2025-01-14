package datanode

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

// This is an unstable it, need to be fixed later
// func TestCompactionSuite(t *testing.T) {
//     suite.Run(t, new(CompactionSuite))
// }

type CompactionSuite struct {
	integration.MiniClusterSuite

	dim int
}

// issue: https://github.com/milvus-io/milvus/issues/30137
func (s *CompactionSuite) TestClearCompactionTask() {
	s.dim = 128
	collName := "test_compaction"
	// generate 1 segment
	pks := s.generateSegment(collName, 1)

	// triggers a compaction
	// restart a datacoord
	s.compactAndReboot(collName)

	// delete data
	// flush -> won't timeout
	s.deleteAndFlush(pks, collName)
}

func (s *CompactionSuite) deleteAndFlush(pks []int64, collection string) {
	ctx := context.Background()

	expr := fmt.Sprintf("%s in [%s]", integration.Int64Field, strings.Join(lo.Map(pks, func(pk int64, _ int) string { return strconv.FormatInt(pk, 10) }), ","))
	log.Info("========================delete expr==================",
		zap.String("expr", expr),
	)
	deleteResp, err := s.Cluster.Proxy.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collection,
		Expr:           expr,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(deleteResp.GetStatus()))
	s.Require().EqualValues(len(pks), deleteResp.GetDeleteCnt())

	log.Info("=========================Data flush=========================")

	flushResp, err := s.Cluster.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
		CollectionNames: []string{collection},
	})
	s.NoError(err)
	segmentLongArr, has := flushResp.GetCollSegIDs()[collection]
	s.Require().True(has)
	segmentIDs := segmentLongArr.GetData()
	s.Require().Empty(segmentLongArr)
	s.Require().True(has)

	flushTs, has := flushResp.GetCollFlushTs()[collection]
	s.True(has)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	log.Info("=========================Wait for flush for 2min=========================")
	s.WaitForFlush(ctx, segmentIDs, flushTs, "", collection)
	log.Info("=========================Data flush done=========================")
}

func (s *CompactionSuite) compactAndReboot(collection string) {
	ctx := context.Background()
	// create index and wait for index done
	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexFaissIDMap, metric.IP),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createIndexStatus))

	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	s.WaitForIndexBuilt(ctxTimeout, collection, integration.FloatVecField)

	// get collectionID
	coll, err := s.Cluster.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(coll.GetStatus()))
	s.Require().EqualValues(coll.GetCollectionName(), collection)

	collID := coll.GetCollectionID()
	compactionResp, err := s.Cluster.Proxy.ManualCompaction(context.TODO(), &milvuspb.ManualCompactionRequest{
		CollectionID: collID,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(coll.GetStatus()))
	// make sure compaction is triggerred successfully
	s.Require().NotEqualValues(-1, compactionResp.GetCompactionID())
	s.Require().EqualValues(1, compactionResp.GetCompactionPlanCount())

	compactID := compactionResp.GetCompactionID()
	stateResp, err := s.Cluster.Proxy.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
		CompactionID: compactID,
	})

	s.Require().NoError(err)
	s.Require().True(merr.Ok(stateResp.GetStatus()))

	compactionSubmitted := func() bool {
		resp, err := s.Cluster.DataNode.GetCompactionState(ctx, &datapb.CompactionStateRequest{})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(resp.GetStatus()))
		return len(resp.GetResults()) > 0
	}

	for !compactionSubmitted() {
		select {
		case <-time.After(1 * time.Minute):
			s.FailNow("failed to wait compaction submitted after 1 minite")
		case <-time.After(500 * time.Millisecond):
		}
	}

	planResp, err := s.Cluster.Proxy.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{
		CompactionID: compactID,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(planResp.GetStatus()))
	s.Require().Equal(1, len(planResp.GetMergeInfos()))

	// Reboot
	if planResp.GetMergeInfos()[0].GetTarget() == int64(-1) {
		s.Cluster.StopDataCoord()
		s.Cluster.StartDataCoord()

		stateResp, err = s.Cluster.Proxy.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compactID,
		})

		s.Require().NoError(err)
		s.Require().True(merr.Ok(stateResp.GetStatus()))
		s.Require().EqualValues(0, stateResp.GetTimeoutPlanNo())
		s.Require().EqualValues(0, stateResp.GetExecutingPlanNo())
		s.Require().EqualValues(0, stateResp.GetCompletedPlanNo())
		s.Require().EqualValues(0, stateResp.GetFailedPlanNo())
	}
}

func (s *CompactionSuite) generateSegment(collection string, segmentCount int) []int64 {
	c := s.Cluster

	schema := integration.ConstructSchema(collection, s.dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	err = merr.Error(createCollectionStatus)
	s.Require().NoError(err)

	rowNum := 3000
	pks := []int64{}
	for i := 0; i < segmentCount; i++ {
		log.Info("=========================Data insertion=========================", zap.Any("count", i))
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, s.dim)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.Proxy.Insert(context.TODO(), &milvuspb.InsertRequest{
			CollectionName: collection,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.GetStatus()))
		s.Require().EqualValues(rowNum, insertResult.GetInsertCnt())
		s.Require().EqualValues(rowNum, len(insertResult.GetIDs().GetIntId().GetData()))

		pks = append(pks, insertResult.GetIDs().GetIntId().GetData()...)

		log.Info("=========================Data flush=========================", zap.Any("count", i))
		flushResp, err := c.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
			CollectionNames: []string{collection},
		})
		s.NoError(err)
		segmentLongArr, has := flushResp.GetCollSegIDs()[collection]
		s.Require().True(has)
		segmentIDs := segmentLongArr.GetData()
		s.Require().NotEmpty(segmentLongArr)
		s.Require().True(has)

		flushTs, has := flushResp.GetCollFlushTs()[collection]
		s.True(has)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.WaitForFlush(ctx, segmentIDs, flushTs, "", collection)
		log.Info("=========================Data flush done=========================", zap.Any("count", i))
	}
	log.Info("=========================Data insertion finished=========================")

	segments, err := c.MetaWatcher.ShowSegments()
	s.Require().NoError(err)
	s.Require().Equal(segmentCount, len(segments))
	lo.ForEach(segments, func(info *datapb.SegmentInfo, _ int) {
		s.Require().Equal(commonpb.SegmentState_Flushed, info.GetState())
		s.Require().EqualValues(3000, info.GetNumOfRows())
	})

	return pks[:300]
}
