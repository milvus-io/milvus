package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"golang.org/x/net/context"
)

func (ds *DataService) RegisterNode(context.Context, *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return nil, nil
}
func (ds *DataService) Flush(context.Context, *datapb.FlushRequest) (*commonpb.Status, error) {
	return nil, nil
}
func (ds *DataService) AssignSegmentID(ctx context.Context, request *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	//for _, req := range request.SegIDRequests {
	//	segmentID, retCount, expireTs, err := ds.segAllocator.AllocSegment(req.CollectionID, req.PartitionID, req.ChannelID, int(req.Count))
	//	if err != nil {
	//		log.Printf()
	//	}
	//}
	return nil, nil
}
func (ds *DataService) ShowSegments(context.Context, *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return nil, nil

}
func (ds *DataService) GetSegmentStates(context.Context, *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return nil, nil

}
func (ds *DataService) GetInsertBinlogPaths(context.Context, *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return nil, nil

}
func (ds *DataService) GetInsertChannels(context.Context, *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return nil, nil

}
