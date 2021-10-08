package proxy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type QueryCoordMockOption func(mock *QueryCoordMock)

type queryCoordShowCollectionsFuncType func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)

func SetQueryCoordShowCollectionsFunc(f queryCoordShowCollectionsFuncType) QueryCoordMockOption {
	return func(mock *QueryCoordMock) {
		mock.showCollectionsFunc = f
	}
}

type QueryCoordMock struct {
	nodeID  typeutil.UniqueID
	address string

	state atomic.Value // internal.StateCode

	collectionIDs       []int64
	inMemoryPercentages []int64
	colMtx              sync.RWMutex

	showCollectionsFunc queryCoordShowCollectionsFuncType
	getMetricsFunc      getMetricsFuncType

	statisticsChannel string
	timeTickChannel   string
}

func (coord *QueryCoordMock) updateState(state internalpb.StateCode) {
	coord.state.Store(state)
}

func (coord *QueryCoordMock) getState() internalpb.StateCode {
	return coord.state.Load().(internalpb.StateCode)
}

func (coord *QueryCoordMock) healthy() bool {
	return coord.getState() == internalpb.StateCode_Healthy
}

func (coord *QueryCoordMock) Init() error {
	coord.updateState(internalpb.StateCode_Initializing)
	return nil
}

func (coord *QueryCoordMock) Start() error {
	defer coord.updateState(internalpb.StateCode_Healthy)

	return nil
}

func (coord *QueryCoordMock) Stop() error {
	defer coord.updateState(internalpb.StateCode_Abnormal)

	return nil
}

func (coord *QueryCoordMock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    coord.nodeID,
			Role:      typeutil.QueryCoordRole,
			StateCode: coord.getState(),
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (coord *QueryCoordMock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.statisticsChannel,
	}, nil
}

func (coord *QueryCoordMock) Register() error {
	return nil
}

func (coord *QueryCoordMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.timeTickChannel,
	}, nil
}

func (coord *QueryCoordMock) ResetShowCollectionsFunc() {
	coord.showCollectionsFunc = nil
}

func (coord *QueryCoordMock) SetShowCollectionsFunc(f queryCoordShowCollectionsFuncType) {
	coord.showCollectionsFunc = f
}

func (coord *QueryCoordMock) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	if !coord.healthy() {
		return &querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.showCollectionsFunc != nil {
		return coord.showCollectionsFunc(ctx, req)
	}

	coord.colMtx.RLock()
	defer coord.colMtx.RUnlock()

	resp := &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionIDs:       coord.collectionIDs,
		InMemoryPercentages: coord.inMemoryPercentages,
	}

	return resp, nil
}

func (coord *QueryCoordMock) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	if !coord.healthy() {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "unhealthy",
		}, nil
	}

	coord.colMtx.Lock()
	defer coord.colMtx.Unlock()

	for _, colID := range coord.collectionIDs {
		if req.CollectionID == colID {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("collection %v already loaded", req.CollectionID),
			}, nil
		}
	}

	coord.collectionIDs = append(coord.collectionIDs, req.CollectionID)
	coord.inMemoryPercentages = append(coord.inMemoryPercentages, 100)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *QueryCoordMock) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if !coord.healthy() {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "unhealthy",
		}, nil
	}

	coord.colMtx.Lock()
	defer coord.colMtx.Unlock()

	for i := len(coord.collectionIDs) - 1; i >= 0; i-- {
		if req.CollectionID == coord.collectionIDs[i] {
			coord.collectionIDs = append(coord.collectionIDs[:i], coord.collectionIDs[i+1:]...)
			coord.inMemoryPercentages = append(coord.inMemoryPercentages[:i], coord.inMemoryPercentages[i+1:]...)

			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			}, nil
		}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    fmt.Sprintf("collection %v not loaded", req.CollectionID),
	}, nil
}

func (coord *QueryCoordMock) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	if !coord.healthy() {
		return &querypb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	panic("implement me")
}

func (coord *QueryCoordMock) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	if !coord.healthy() {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "unhealthy",
		}, nil
	}

	panic("implement me")
}

func (coord *QueryCoordMock) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	if !coord.healthy() {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "unhealthy",
		}, nil
	}

	panic("implement me")
}

func (coord *QueryCoordMock) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	if !coord.healthy() {
		return &querypb.CreateQueryChannelResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	panic("implement me")
}

func (coord *QueryCoordMock) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	if !coord.healthy() {
		return &querypb.GetPartitionStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	panic("implement me")
}

func (coord *QueryCoordMock) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	if !coord.healthy() {
		return &querypb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	panic("implement me")
}

func (coord *QueryCoordMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !coord.healthy() {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.getMetricsFunc != nil {
		return coord.getMetricsFunc(ctx, req)
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
		Response:      "",
		ComponentName: "",
	}, nil
}

func NewQueryCoordMock(opts ...QueryCoordMockOption) *QueryCoordMock {
	coord := &QueryCoordMock{
		nodeID:              UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		address:             funcutil.GenRandomStr(), // TODO(dragondriver): random address
		state:               atomic.Value{},
		collectionIDs:       make([]int64, 0),
		inMemoryPercentages: make([]int64, 0),
		colMtx:              sync.RWMutex{},
		statisticsChannel:   funcutil.GenRandomStr(),
		timeTickChannel:     funcutil.GenRandomStr(),
	}

	for _, opt := range opts {
		opt(coord)
	}

	return coord
}
