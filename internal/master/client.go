package master

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	writerclient "github.com/zilliztech/milvus-distributed/internal/writenode/client"
)

type WriteNodeClient interface {
	FlushSegment(segmentID UniqueID, collectionID UniqueID, partitionTag string, timestamp Timestamp) error
	DescribeSegment(segmentID UniqueID) (*writerclient.SegmentDescription, error)
	GetInsertBinlogPaths(segmentID UniqueID) (map[UniqueID][]string, error)
}

type MockWriteNodeClient struct {
	segmentID    UniqueID
	flushTime    time.Time
	partitionTag string
	timestamp    Timestamp
	collectionID UniqueID
	lock         sync.RWMutex
}

func (m *MockWriteNodeClient) FlushSegment(segmentID UniqueID, collectionID UniqueID, partitionTag string, timestamp Timestamp) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.flushTime = time.Now()
	m.segmentID = segmentID
	m.collectionID = collectionID
	m.partitionTag = partitionTag
	m.timestamp = timestamp
	return nil
}

func (m *MockWriteNodeClient) DescribeSegment(segmentID UniqueID) (*writerclient.SegmentDescription, error) {
	now := time.Now()
	m.lock.RLock()
	defer m.lock.RUnlock()
	if now.Sub(m.flushTime).Seconds() > 2 {
		return &writerclient.SegmentDescription{
			SegmentID: segmentID,
			IsClosed:  true,
			OpenTime:  0,
			CloseTime: 1,
		}, nil
	}
	return &writerclient.SegmentDescription{
		SegmentID: segmentID,
		IsClosed:  false,
		OpenTime:  0,
		CloseTime: 1,
	}, nil
}

func (m *MockWriteNodeClient) GetInsertBinlogPaths(segmentID UniqueID) (map[UniqueID][]string, error) {
	return map[UniqueID][]string{
		1:   {"/binlog/insert/file_1"},
		100: {"/binlog/insert/file_100"},
	}, nil
}

type BuildIndexClient interface {
	BuildIndex(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error)
	GetIndexStates(indexID UniqueID) (*indexpb.IndexStatesResponse, error)
	GetIndexFilePaths(indexID UniqueID) ([]string, error)
}

type MockBuildIndexClient struct {
	buildTime time.Time
}

func (m *MockBuildIndexClient) BuildIndex(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error) {
	m.buildTime = time.Now()
	return 1, nil
}

func (m *MockBuildIndexClient) GetIndexStates(indexID UniqueID) (*indexpb.IndexStatesResponse, error) {
	now := time.Now()
	if now.Sub(m.buildTime).Seconds() > 2 {
		return &indexpb.IndexStatesResponse{
			IndexID: indexID,
			State:   commonpb.IndexState_FINISHED,
		}, nil
	}
	return &indexpb.IndexStatesResponse{
		IndexID: 1,
		State:   commonpb.IndexState_INPROGRESS,
	}, nil
}

func (m *MockBuildIndexClient) GetIndexFilePaths(indexID UniqueID) ([]string, error) {
	return []string{"/binlog/index/file_1", "/binlog/index/file_2", "/binlog/index/file_3"}, nil
}

type LoadIndexClient interface {
	LoadIndex(indexPaths []string, segmentID int64, fieldID int64, fieldName string, indexParams map[string]string) error
}

type MockLoadIndexClient struct {
}

func (m *MockLoadIndexClient) LoadIndex(indexPaths []string, segmentID int64, fieldID int64, fieldName string, indexParams map[string]string) error {
	return nil
}
