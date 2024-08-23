package channelbalance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

func TestChannelBalanceSuite(t *testing.T) {
	if streamingutil.IsStreamingServiceEnabled() {
		t.Skip("skip channel balance test in streaming service mode")
	}
	suite.Run(t, new(ChannelBalanceSuite))
}

type ChannelBalanceSuite struct {
	integration.MiniClusterSuite

	dim int
}

func (s *ChannelBalanceSuite) initSuite() {
	s.dim = 128
}

func (s *ChannelBalanceSuite) TestReAssignmentWithDNReboot() {
	s.initSuite()

	// Init with 1 DC, 1 DN, Create 10 collections
	// check collection's channel been watched by DN by insert&flush
	collections := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		collections = append(collections, fmt.Sprintf("test_balance_%d", i))
	}

	lo.ForEach(collections, func(collection string, _ int) {
		// create collection will triggers channel assignments.
		s.createCollection(collection)
		s.insert(collection, 3000)
	})

	s.flushCollections(collections)

	// reboot DN
	s.Cluster.StopAllDataNodes()
	s.Cluster.AddDataNode()

	// check channels reassignments by insert/delete & flush
	lo.ForEach(collections, func(collection string, _ int) {
		s.insert(collection, 3000)
	})
	s.flushCollections(collections)
}

func (s *ChannelBalanceSuite) createCollection(collection string) {
	schema := integration.ConstructSchema(collection, s.dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := s.Cluster.Proxy.CreateCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createCollectionStatus))
}

func (s *ChannelBalanceSuite) flushCollections(collections []string) {
	log.Info("=========================Data flush=========================")
	flushResp, err := s.Cluster.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
		CollectionNames: collections,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(flushResp.GetStatus()))

	for collection, segLongArr := range flushResp.GetCollSegIDs() {
		s.Require().NotEmpty(segLongArr)
		segmentIDs := segLongArr.GetData()
		s.Require().NotEmpty(segmentIDs)

		flushTs, has := flushResp.GetCollFlushTs()[collection]
		s.True(has)
		s.NotEmpty(flushTs)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.WaitForFlush(ctx, segmentIDs, flushTs, "", collection)

		// Check segments are flushed
		coll, err := s.Cluster.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			CollectionName: collection,
		})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(coll.GetStatus()))
		s.Require().EqualValues(coll.GetCollectionName(), collection)

		collID := coll.GetCollectionID()
		segments, err := s.Cluster.MetaWatcher.ShowSegments()
		s.Require().NoError(err)

		collSegs := lo.Filter(segments, func(info *datapb.SegmentInfo, _ int) bool {
			return info.GetCollectionID() == collID
		})
		lo.ForEach(collSegs, func(info *datapb.SegmentInfo, _ int) {
			s.Require().Contains([]commonpb.SegmentState{commonpb.SegmentState_Flushed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Dropped}, info.GetState())
		})
	}
	log.Info("=========================Data flush done=========================")
}

func (s *ChannelBalanceSuite) insert(collection string, numRows int) {
	log.Info("=========================Data insertion=========================")
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, numRows, s.dim)
	hashKeys := integration.GenerateHashKeys(numRows)
	insertResult, err := s.Cluster.Proxy.Insert(context.TODO(), &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(numRows),
	})
	s.Require().NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))
	s.Require().EqualValues(numRows, insertResult.GetInsertCnt())
	s.Require().EqualValues(numRows, len(insertResult.GetIDs().GetIntId().GetData()))
}
