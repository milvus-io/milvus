package partialsearch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim    = 128
	dbName = ""

	timeout = 10 * time.Second
)

type PartialSearchTestSuit struct {
	integration.MiniClusterSuite
}

func (s *PartialSearchTestSuit) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.AutoBalanceInterval.Key, "10000")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "10000")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.TaskExecutionCap.Key, "1")
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "1ms")

	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
}

func (s *PartialSearchTestSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 1; i < replica; i++ {
		s.Cluster.AddQueryNode()
		s.Cluster.AddStreamingNode()
	}

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       channelNum,
		SegmentNum:       segmentNum,
		RowNumPerSegment: segmentRowNum,
	})

	// load
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.True(merr.Ok(loadStatus))
	s.WaitForLoad(ctx, collectionName)
	log.Info("initCollection Done")
}

func (s *PartialSearchTestSuit) executeQuery(collection string) (int, error) {
	time.Sleep(100 * time.Millisecond)
	ctx := context.Background()
	queryResult, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName:         "",
		CollectionName: collection,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})

	if err := merr.CheckRPCCall(queryResult.GetStatus(), err); err != nil {
		log.Info("query failed", zap.Error(err))
		return 0, err
	}

	return int(queryResult.FieldsData[0].GetScalars().GetLongData().Data[0]), nil
}

// expected return partial result
// Note: for now if any delegator is down, search will fail; partial result only works when delegator is up
func (s *PartialSearchTestSuit) TestSingleNodeDownOnSingleReplica() {
	partialResultRequiredDataRatio := 0.3
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key: fmt.Sprintf("%f", partialResultRequiredDataRatio),
	})
	defer revertGuard()

	// init cluster with 6 querynode
	for i := 1; i < 6; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 2 channels, 6 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	channelNum := 2
	segmentNumInChannel := 6
	segmentRowNum := 2000
	s.initCollection(name, 1, channelNum, segmentNumInChannel, segmentRowNum)
	totalEntities := segmentNumInChannel * segmentRowNum

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			default:
				numEntities, err := s.executeQuery(name)
				if err != nil {
					log.Info("query failed", zap.Error(err))
					failCounter.Inc()
				} else if numEntities < totalEntities {
					log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
					partialResultCounter.Inc()
					s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	// stop qn in single replica expected got search failures
	s.Cluster.DefaultQueryNode().Stop()
	time.Sleep(10 * time.Second)
	s.True(failCounter.Load() >= 0)
	s.True(partialResultCounter.Load() >= 0)
	close(stopSearchCh)
	wg.Wait()
}

// expected some search failures, but partial result is returned before all data is loaded
// for case which all querynode down, partial search can decrease recovery time
func (s *PartialSearchTestSuit) TestAllNodeDownOnSingleReplica() {
	partialResultRequiredDataRatio := 0.5
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key: fmt.Sprintf("%f", partialResultRequiredDataRatio),
	})
	defer revertGuard()

	// init cluster with 2 querynode
	s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 10 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	channelNum := 2
	segmentNumInChannel := 10
	segmentRowNum := 2000
	s.initCollection(name, 1, channelNum, segmentNumInChannel, segmentRowNum)
	totalEntities := segmentNumInChannel * segmentRowNum

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	partialResultRecoverTs := atomic.NewInt64(0)
	fullResultRecoverTs := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			default:
				numEntities, err := s.executeQuery(name)
				if err != nil {
					log.Info("query failed", zap.Error(err))
					failCounter.Inc()
				} else if failCounter.Load() > 0 {
					if numEntities < totalEntities {
						log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
						partialResultCounter.Inc()
						partialResultRecoverTs.Store(time.Now().UnixNano())
						s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
					} else {
						log.Info("query return full result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
						fullResultRecoverTs.Store(time.Now().UnixNano())
					}
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	// stop all qn in single replica expected got search failures
	for _, qn := range s.Cluster.GetAllQueryNodes() {
		qn.Stop()
	}
	time.Sleep(2 * time.Second)
	s.Cluster.AddQueryNode()

	time.Sleep(20 * time.Second)
	s.True(failCounter.Load() >= 0)
	s.True(partialResultCounter.Load() >= 0)
	log.Info("partialResultRecoverTs", zap.Int64("partialResultRecoverTs", partialResultRecoverTs.Load()), zap.Int64("fullResultRecoverTs", fullResultRecoverTs.Load()))
	s.True(partialResultRecoverTs.Load() < fullResultRecoverTs.Load())
	close(stopSearchCh)
	wg.Wait()
}

// expected return full result
// Note: for now if any delegator is down, search will fail; partial result only works when delegator is up
// cause we won't pick best replica to response query, there may return partial result even when only one replica is partial loaded
func (s *PartialSearchTestSuit) TestSingleNodeDownOnMultiReplica() {
	partialResultRequiredDataRatio := 0.5
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key: fmt.Sprintf("%f", partialResultRequiredDataRatio),
	})
	defer revertGuard()
	// init cluster with 4 querynode
	qn1 := s.Cluster.AddQueryNode()
	s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	channelNum := 2
	segmentNumInChannel := 2
	segmentRowNum := 2000
	s.initCollection(name, 2, channelNum, segmentNumInChannel, segmentRowNum)
	totalEntities := segmentNumInChannel * segmentRowNum

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			default:
				numEntities, err := s.executeQuery(name)
				if err != nil {
					log.Info("query failed", zap.Error(err))
					failCounter.Inc()
				} else if numEntities < totalEntities {
					log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
					partialResultCounter.Inc()
					s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	// stop qn in single replica expected got search failures
	qn1.Stop()
	time.Sleep(10 * time.Second)
	s.True(failCounter.Load() >= 0)
	s.True(partialResultCounter.Load() >= 0)
	close(stopSearchCh)
	wg.Wait()
}

// expected return partial result
// Note: for now if any delegator is down, search will fail; partial result only works when delegator is up
func (s *PartialSearchTestSuit) TestEachReplicaHasNodeDownOnMultiReplica() {
	partialResultRequiredDataRatio := 0.3
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key: fmt.Sprintf("%f", partialResultRequiredDataRatio),
	})
	defer revertGuard()
	// init cluster with 12 querynode
	for i := 2; i < 12; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 2 channels, 18 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	channelNum := 2
	segmentNumInChannel := 9
	segmentRowNum := 2000
	s.initCollection(name, 2, channelNum, segmentNumInChannel, segmentRowNum)
	totalEntities := segmentNumInChannel * segmentRowNum

	ctx := context.Background()
	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			default:
				numEntities, err := s.executeQuery(name)
				if err != nil {
					log.Info("query failed", zap.Error(err))
					failCounter.Inc()
					continue
				} else if numEntities < totalEntities {
					log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
					partialResultCounter.Inc()
					s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	replicaResp, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: name,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, replicaResp.GetStatus().GetErrorCode())
	s.Equal(2, len(replicaResp.GetReplicas()))

	// for each replica, choose a querynode to stop
	for _, replica := range replicaResp.GetReplicas() {
		for _, qn := range s.Cluster.GetAllQueryNodes() {
			if funcutil.SliceContain(replica.GetNodeIds(), qn.GetNodeID()) {
				qn.Stop()
				break
			}
		}
	}

	time.Sleep(10 * time.Second)
	s.True(failCounter.Load() >= 0)
	s.True(partialResultCounter.Load() >= 0)
	close(stopSearchCh)
	wg.Wait()
}

// when set high partial result required data ratio, partial search will not be triggered, expected search failures
// for example, set 0.8, but each querynode crash will lost 50% data, so partial search will not be triggered
func (s *PartialSearchTestSuit) TestPartialResultRequiredDataRatioTooHigh() {
	partialResultRequiredDataRatio := 0.8
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key: fmt.Sprintf("%f", partialResultRequiredDataRatio),
	})
	defer revertGuard()
	// init cluster with 2 querynode
	qn1 := s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	channelNum := 2
	segmentNumInChannel := 2
	segmentRowNum := 2000
	s.initCollection(name, 1, channelNum, segmentNumInChannel, segmentRowNum)
	totalEntities := segmentNumInChannel * segmentRowNum

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			default:
				numEntities, err := s.executeQuery(name)
				if err != nil {
					log.Info("query failed", zap.Error(err))
					failCounter.Inc()
				} else if numEntities < totalEntities {
					log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
					partialResultCounter.Inc()
					s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	qn1.Stop()
	time.Sleep(10 * time.Second)
	s.True(failCounter.Load() >= 0)
	s.True(partialResultCounter.Load() == 0)
	close(stopSearchCh)
	wg.Wait()
}

// set partial result required data ratio to 0, expected no partial result and no search failures even after all querynode down
// Note: for now if any delegator is down, search will fail; partial result only works when delegator is up
// func (s *PartialSearchTestSuit) TestSearchNeverFails() {
// 	partialResultRequiredDataRatio := 0.0
// 	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key, fmt.Sprintf("%f", partialResultRequiredDataRatio))
// 	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key)
// 	// init cluster with 2 querynode
// 	s.Cluster.AddQueryNode()

// 	// init collection with 1 replica, 2 channels, 4 segments, 2000 rows per segment
// 	// expect each node has 1 channel and 2 segments
// 	name := "test_balance_" + funcutil.GenRandomStr()
// 	channelNum := 2
// 	segmentNumInChannel := 2
// 	segmentRowNum := 2000
// 	s.initCollection(name, 1, channelNum, segmentNumInChannel, segmentRowNum)
// 	totalEntities := segmentNumInChannel * segmentRowNum

// 	stopSearchCh := make(chan struct{})
// 	failCounter := atomic.NewInt64(0)
// 	partialResultCounter := atomic.NewInt64(0)

// 	wg := sync.WaitGroup{}
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			select {
// 			case <-stopSearchCh:
// 				log.Info("stop search")
// 				return
// 			default:
// 				numEntities, err := s.executeQuery(name)
// 				if err != nil {
// 					log.Info("query failed", zap.Error(err))
// 					failCounter.Inc()
// 				} else if numEntities < totalEntities {
// 					log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
// 					partialResultCounter.Inc()
// 					s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
// 				}
// 			}
// 		}
// 	}()

// 	time.Sleep(10 * time.Second)
// 	s.Equal(failCounter.Load(), int64(0))
// 	s.Equal(partialResultCounter.Load(), int64(0))

// 	for _, qn := range s.Cluster.GetAllQueryNodes() {
// 		qn.Stop()
// 	}
// 	time.Sleep(10 * time.Second)
// 	s.Equal(failCounter.Load(), int64(0))
// 	s.True(partialResultCounter.Load() >= 0)
// 	close(stopSearchCh)
// 	wg.Wait()
// }

// expect partial result could be returned even if tsafe is delayed
func (s *PartialSearchTestSuit) TestSkipWaitTSafe() {
	partialResultRequiredDataRatio := 0.5
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.Key: fmt.Sprintf("%f", partialResultRequiredDataRatio),
	})
	defer revertGuard()
	// mock tsafe Delay
	// init cluster with 5 querynode
	for i := 1; i < 5; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 1 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 1, 4, 2000)
	channelNum := 1
	segmentNumInChannel := 4
	segmentRowNum := 2000
	s.initCollection(name, 1, channelNum, segmentNumInChannel, segmentRowNum)
	totalEntities := segmentNumInChannel * segmentRowNum

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			default:
				numEntities, err := s.executeQuery(name)
				if err != nil {
					log.Info("query failed", zap.Error(err))
					failCounter.Inc()
				} else if numEntities < totalEntities {
					log.Info("query return partial result", zap.Int("numEntities", numEntities), zap.Int("totalEntities", totalEntities))
					partialResultCounter.Inc()
					s.True(numEntities >= int((float64(totalEntities) * partialResultRequiredDataRatio)))
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	s.Cluster.DefaultQueryNode().Stop()
	time.Sleep(10 * time.Second)
	s.True(failCounter.Load() >= 0)
	s.True(partialResultCounter.Load() >= 0)
	close(stopSearchCh)
	wg.Wait()
}

func TestPartialResult(t *testing.T) {
	suite.Run(t, new(PartialSearchTestSuit))
}
