package master

import (
	"time"

	buildindexclient "github.com/zilliztech/milvus-distributed/internal/indexbuilder/client"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
	writerclient "github.com/zilliztech/milvus-distributed/internal/writenode/client"
)

type WriteNodeClient interface {
	FlushSegment(segmentID UniqueID) error
	DescribeSegment(segmentID UniqueID) (*writerclient.SegmentDescription, error)
	GetInsertBinlogPaths(segmentID UniqueID) (map[UniqueID][]string, error)
}

type MockWriteNodeClient struct {
	segmentID UniqueID
	flushTime time.Time
}

func (m *MockWriteNodeClient) FlushSegment(segmentID UniqueID) error {
	m.flushTime = time.Now()
	m.segmentID = segmentID
	return nil
}

func (m *MockWriteNodeClient) DescribeSegment(segmentID UniqueID) (*writerclient.SegmentDescription, error) {
	now := time.Now()
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
	BuildIndexWithoutID(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error)
	DescribeIndex(indexID UniqueID) (*buildindexclient.IndexDescription, error)
	GetIndexFilePaths(indexID UniqueID) ([]string, error)
}

type MockBuildIndexClient struct {
	buildTime time.Time
}

func (m *MockBuildIndexClient) BuildIndexWithoutID(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error) {
	m.buildTime = time.Now()
	return 1, nil
}

func (m *MockBuildIndexClient) DescribeIndex(indexID UniqueID) (*buildindexclient.IndexDescription, error) {
	now := time.Now()
	if now.Sub(m.buildTime).Seconds() > 2 {
		return &buildindexclient.IndexDescription{
			ID:                1,
			Status:            indexbuilderpb.IndexStatus_FINISHED,
			EnqueueTime:       time.Now(),
			ScheduleTime:      time.Now(),
			BuildCompleteTime: time.Now(),
		}, nil
	}
	return &buildindexclient.IndexDescription{
		ID:                1,
		Status:            indexbuilderpb.IndexStatus_INPROGRESS,
		EnqueueTime:       time.Now(),
		ScheduleTime:      time.Now(),
		BuildCompleteTime: time.Now(),
	}, nil
}

func (m *MockBuildIndexClient) GetIndexFilePaths(indexID UniqueID) ([]string, error) {
	return []string{"/binlog/index/file_1", "/binlog/index/file_2", "/binlog/index/file_3"}, nil
}

type LoadIndexClient interface {
	LoadIndex(indexPaths []string, segmentID int64, fieldID int64, fieldName string) error
}

type MockLoadIndexClient struct {
}

func (m *MockLoadIndexClient) LoadIndex(indexPaths []string, segmentID int64, fieldID int64, fieldName string) error {
	return nil
}
