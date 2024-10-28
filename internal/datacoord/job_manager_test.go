package datacoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type jobManagerSuite struct {
	suite.Suite
}

func Test_jobManagerSuite(t *testing.T) {
	suite.Run(t, new(jobManagerSuite))
}

func (s *jobManagerSuite) TestJobManager_triggerStatsTaskLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	var start int64
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		start++
		return start, nil
	})
	Params.Save(Params.DataCoordCfg.TaskCheckInterval.Key, "1")

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)

	mt := &meta{
		collections: map[UniqueID]*collectionInfo{
			1: {
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:  100,
							Name:     "pk",
							DataType: schemapb.DataType_Int64,
						},
						{
							FieldID:  101,
							Name:     "var",
							DataType: schemapb.DataType_VarChar,
							TypeParams: []*commonpb.KeyValuePair{
								{
									Key: "enable_match", Value: "true",
								},
								{
									Key: "enable_tokenizer", Value: "true",
								},
							},
						},
					},
				},
			},
		},
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				10: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           10,
						CollectionID: 1,
						PartitionID:  2,
						IsSorted:     false,
						State:        commonpb.SegmentState_Flushed,
					},
				},
				20: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           20,
						CollectionID: 1,
						PartitionID:  2,
						IsSorted:     true,
						State:        commonpb.SegmentState_Flushed,
					},
				},
			},
		},
		statsTaskMeta: &statsTaskMeta{
			ctx:     ctx,
			catalog: catalog,
			tasks:   make(map[int64]*indexpb.StatsTask),
		},
	}

	jm := &statsJobManager{
		ctx:    ctx,
		cancel: cancel,
		loopWg: sync.WaitGroup{},
		mt:     mt,
		scheduler: &taskScheduler{
			allocator: alloc,
			tasks:     make(map[int64]Task),
			meta:      mt,
			taskStats: expirable.NewLRU[UniqueID, Task](1024, nil, time.Minute*5),
		},
		allocator: alloc,
	}

	jm.loopWg.Add(1)
	go jm.triggerStatsTaskLoop()

	time.Sleep(2 * time.Second)
	cancel()

	jm.loopWg.Wait()

	s.Equal(2, len(jm.scheduler.tasks))
}
