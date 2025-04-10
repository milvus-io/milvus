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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim    = 128
	dbName = ""
)

type PartialSearchTestSuit struct {
	integration.MiniClusterSuite
}

func (s *PartialSearchTestSuit) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	// slow down segment checker and channel checker
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.SegmentCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.AutoBalanceInterval.Key, "10000")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "10000")

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.TaskExecutionCap.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.CheckExecutedFlagInterval.Key, "1000")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *PartialSearchTestSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       channelNum,
		SegmentNum:       segmentNum,
		RowNumPerSegment: segmentRowNum,
	})

	for i := 1; i < replica; i++ {
		s.Cluster.AddQueryNode()
	}

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
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

func (s *PartialSearchTestSuit) TestSingleNodeDownOnSingleReplicaWithGlobalConfig() {
	// enable partial search
	paramtable.Get().Save(paramtable.Get().ProxyCfg.PartialResultRequiredDataRatio.Key, "0.3")
	defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.PartialResultRequiredDataRatio.Key)

	// init cluster with 6 querynode
	for i := 1; i < 6; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 2 channels, 6 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 9, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	// stop qn in single replica expected got search failures
	s.Cluster.QueryNode.Stop()
	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.True(partialResultCounter.Load() > 0)
	close(stopSearchCh)
	wg.Wait()
}

// expected return partial result, no search failures
func (s *PartialSearchTestSuit) TestSingleNodeDownOnSingleReplica() {
	// init cluster with 6 querynode
	for i := 1; i < 6; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 2 channels, 6 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 9, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.3

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() >= searchReq.GetPartialResultRequiredDataRatio())
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	// stop qn in single replica expected got search failures
	s.Cluster.QueryNode.Stop()
	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.True(partialResultCounter.Load() > 0)
	close(stopSearchCh)
	wg.Wait()
}

// expected some search failures, but partial result is returned before all data is loaded
// for case which all querynode down, partial search can decrease recovery time
func (s *PartialSearchTestSuit) TestAllNodeDownOnSingleReplica() {
	// init cluster with 2 querynode
	s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 10 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 10, 2000)

	ctx := context.Background()
	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	partialResultCounter := atomic.NewInt64(0)

	var partialResultRecoverTs int64
	var fullResultRecoverTs int64
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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.5

				searchCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if failCounter.Load() > 0 {
					if searchResult.GetAccessedDataRatio() < 1.0 {
						log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
						partialResultCounter.Inc()
						partialResultRecoverTs = time.Now().UnixNano()
						s.True(searchResult.GetAccessedDataRatio() >= searchReq.GetPartialResultRequiredDataRatio())
					} else {
						log.Info("search return full result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
						fullResultRecoverTs = time.Now().UnixNano()
						s.True(searchResult.GetAccessedDataRatio() == 1.0)
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
	s.Cluster.AddQueryNode()

	time.Sleep(20 * time.Second)
	s.True(failCounter.Load() > 0)
	s.True(partialResultCounter.Load() > 0)
	s.True(partialResultRecoverTs < fullResultRecoverTs)
	close(stopSearchCh)
	wg.Wait()
}

// expected return full result, no search failures
func (s *PartialSearchTestSuit) TestSingleNodeDownOnMultiReplica() {
	// init cluster with 4 querynode
	qn1 := s.Cluster.AddQueryNode()
	s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 2, 2, 2, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.5

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() == 1.0)
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
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))
	close(stopSearchCh)
	wg.Wait()
}

// expected return partial result, no search failures
func (s *PartialSearchTestSuit) TestEachReplicaHasNodeDownOnMultiReplica() {
	// init cluster with 12 querynode
	for i := 2; i < 12; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 2 channels, 18 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 2, 2, 9, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.3

				searchCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() >= searchReq.GetPartialResultRequiredDataRatio())
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	replicaResp, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: name,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, replicaResp.GetStatus().GetErrorCode())
	s.Equal(2, len(replicaResp.GetReplicas()))

	// for each replica, choose a querynode to stop
	for _, replica := range replicaResp.GetReplicas() {
		for _, qn := range s.Cluster.GetAllQueryNodes() {
			if funcutil.SliceContain(replica.GetNodeIds(), qn.GetQueryNode().GetNodeID()) {
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
	// init cluster with 2 querynode
	qn1 := s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.8

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() >= searchReq.GetPartialResultRequiredDataRatio())
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	qn1.Stop()
	time.Sleep(10 * time.Second)
	s.True(failCounter.Load() > 0)
	s.True(partialResultCounter.Load() == 0)
	close(stopSearchCh)
	wg.Wait()
}

// set partial result required data ratio to 0, expected no partial result and no search failures even after all querynode down
func (s *PartialSearchTestSuit) TestSearchNeverFails() {
	// init cluster with 2 querynode
	s.Cluster.AddQueryNode()

	// init collection with 1 replica, 2 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() >= searchReq.GetPartialResultRequiredDataRatio())
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	for _, qn := range s.Cluster.GetAllQueryNodes() {
		qn.Stop()
	}
	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.True(partialResultCounter.Load() > 0)
	close(stopSearchCh)
	wg.Wait()
}

// setup 6 querynode, 1 replica, 1 channel, 5 segments, 2000 rows per segment
// each querynode load 1 segment, then the access data ratio should be 0.8 after 1 querynode down
func (s *PartialSearchTestSuit) TestAccessDataRatio() {
	// init cluster with 5 querynode
	for i := 1; i < 5; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 1 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 1, 5, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.5

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() == 0.8)
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	s.Cluster.QueryNode.Stop()
	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.True(partialResultCounter.Load() > 0)
	close(stopSearchCh)
	wg.Wait()
}

func (s *PartialSearchTestSuit) TestSkipWaitTSafe() {
	// mock tsafe Delay
	paramtable.Get().Save(paramtable.Get().ProxyCfg.TimeTickInterval.Key, "10000")
	// init cluster with 5 querynode
	for i := 1; i < 5; i++ {
		s.Cluster.AddQueryNode()
	}

	// init collection with 1 replica, 1 channels, 4 segments, 2000 rows per segment
	// expect each node has 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 1, 5, 2000)

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
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
				searchReq.EnablePartialResult = true
				searchReq.PartialResultRequiredDataRatio = 0.5
				searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
				searchReq.UseDefaultConsistency = true

				searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				searchResult, err := s.Cluster.Proxy.Search(searchCtx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					log.Info("search failed", zap.Error(err))
					failCounter.Inc()
				} else if searchResult.GetAccessedDataRatio() < 1.0 {
					log.Info("search return partial result", zap.Float32("accessed data ratio", searchResult.GetAccessedDataRatio()))
					partialResultCounter.Inc()
					s.True(searchResult.GetAccessedDataRatio() == 0.8)
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.Equal(partialResultCounter.Load(), int64(0))

	s.Cluster.QueryNode.Stop()
	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))
	s.True(partialResultCounter.Load() > 0)
	close(stopSearchCh)
	wg.Wait()
}

func TestPartialResult(t *testing.T) {
	suite.Run(t, new(PartialSearchTestSuit))
}
