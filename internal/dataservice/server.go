package dataservice

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/timesync"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type DataService interface {
	typeutil.Service
	RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
	Flush(req *datapb.FlushRequest) (*commonpb.Status, error)

	AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error)
	ShowSegments(req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error)
	GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
	GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error)

	GetInsertChannels(req *datapb.InsertChannelRequest) (*internalpb2.StringList, error)
	GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error)
	GetPartitionStatistics(req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error)
	GetComponentStates() (*internalpb2.ComponentStates, error)
	GetTimeTickChannel() (*milvuspb.StringResponse, error)
	GetStatisticsChannel() (*milvuspb.StringResponse, error)
}

type (
	datanode struct {
		nodeID  int64
		address struct {
			ip   string
			port int64
		}
		// todo add client
	}

	Server struct {
		ctx              context.Context
		state            internalpb2.StateCode
		client           *etcdkv.EtcdKV
		meta             *meta
		segAllocator     segmentAllocator
		statsHandler     *statsHandler
		insertChannelMgr *insertChannelManager
		allocator        allocator
		msgProducer      *timesync.MsgProducer
		nodeIDCounter    int64
		nodes            []*datanode
		registerFinishCh chan struct{}
		registerMu       sync.RWMutex
	}
)

func CreateServer(ctx context.Context) (*Server, error) {
	return &Server{
		ctx:              ctx,
		state:            internalpb2.StateCode_INITIALIZING,
		insertChannelMgr: newInsertChannelManager(),
		nodeIDCounter:    0,
		nodes:            make([]*datanode, 0),
		registerFinishCh: make(chan struct{}),
	}, nil
}

func (s *Server) Init() error {
	Params.Init()
	s.allocator = newAllocatorImpl()
	if err := s.initMeta(); err != nil {
		return err
	}
	s.statsHandler = newStatsHandler(s.meta)
	segAllocator, err := newSegmentAssigner(s.meta, s.allocator)
	if err != nil {
		return err
	}
	s.segAllocator = segAllocator
	if err = s.initMsgProducer(); err != nil {
		return err
	}
	return nil
}

func (s *Server) initMeta() error {
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})
	if err != nil {
		return err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	s.client = etcdKV
	s.meta, err = newMeta(etcdKV, s.allocator)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) initMsgProducer() error {
	// todo ttstream and peerids
	timeTickBarrier := timesync.NewHardTimeTickBarrier(nil, nil)
	// todo add watchers
	producer, err := timesync.NewTimeSyncMsgProducer(timeTickBarrier)
	if err != nil {
		return err
	}
	s.msgProducer = producer
	return nil
}

func (s *Server) Start() error {
	s.waitDataNodeRegister()
	// todo add load meta from master
	s.msgProducer.Start(s.ctx)
	return nil
}

func (s *Server) waitDataNodeRegister() {
	<-s.registerFinishCh
}

func (s *Server) Stop() error {
	s.msgProducer.Close()
	return nil
}

func (s *Server) GetComponentStates() (*internalpb2.ComponentStates, error) {
	// todo foreach datanode, call GetServiceStates
	return nil, nil
}

func (s *Server) GetTimeTickChannel() (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: Params.TimeTickChannelName,
	}, nil
}

func (s *Server) GetStatisticsChannel() (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: Params.StatisticsChannelName,
	}, nil
}

func (s *Server) RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	s.registerMu.Lock()
	defer s.registerMu.Unlock()
	resp := &datapb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	if !s.checkDataNodeNotExist(req.Address.Ip, req.Address.Port) {
		resp.Status.Reason = fmt.Sprintf("data node with address %s exist", req.Address.String())
		return resp, nil
	}
	s.nodeIDCounter++
	s.nodes = append(s.nodes, &datanode{
		nodeID: s.nodeIDCounter,
		address: struct {
			ip   string
			port int64
		}{ip: req.Address.Ip, port: req.Address.Port},
	})
	if s.nodeIDCounter == Params.DataNodeNum {
		close(s.registerFinishCh)
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	// add init params
	return resp, nil
}

func (s *Server) checkDataNodeNotExist(ip string, port int64) bool {
	for _, node := range s.nodes {
		if node.address.ip == ip || node.address.port == port {
			return false
		}
	}
	return true
}

func (s *Server) Flush(req *datapb.FlushRequest) (*commonpb.Status, error) {
	// todo call datanode flush
	return nil, nil
}

func (s *Server) AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	resp := &datapb.AssignSegIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		SegIDAssignments: make([]*datapb.SegIDAssignment, 0),
	}
	for _, r := range req.SegIDRequests {
		result := &datapb.SegIDAssignment{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			},
		}
		segmentID, retCount, expireTs, err := s.segAllocator.AllocSegment(r.CollectionID, r.PartitionID, r.ChannelName, int(r.Count))
		if err != nil {
			if _, ok := err.(errRemainInSufficient); !ok {
				result.Status.Reason = fmt.Sprintf("allocation of Collection %d, Partition %d, Channel %s, Count %d error:  %s",
					r.CollectionID, r.PartitionID, r.ChannelName, r.Count, err.Error())
				resp.SegIDAssignments = append(resp.SegIDAssignments, result)
				continue
			}

			log.Printf("no enough space for allocation of Collection %d, Partition %d, Channel %s, Count %d",
				r.CollectionID, r.PartitionID, r.ChannelName, r.Count)
			if err = s.openNewSegment(r.CollectionID, r.PartitionID, r.ChannelName); err != nil {
				result.Status.Reason = fmt.Sprintf("open new segment of Collection %d, Partition %d, Channel %s, Count %d error:  %s",
					r.CollectionID, r.PartitionID, r.ChannelName, r.Count, err.Error())
				resp.SegIDAssignments = append(resp.SegIDAssignments, result)
				continue
			}

			segmentID, retCount, expireTs, err = s.segAllocator.AllocSegment(r.CollectionID, r.PartitionID, r.ChannelName, int(r.Count))
			if err != nil {
				result.Status.Reason = fmt.Sprintf("retry allocation of Collection %d, Partition %d, Channel %s, Count %d error:  %s",
					r.CollectionID, r.PartitionID, r.ChannelName, r.Count, err.Error())
				resp.SegIDAssignments = append(resp.SegIDAssignments, result)
				continue
			}
		}

		result.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
		result.CollectionID = r.CollectionID
		result.SegID = segmentID
		result.PartitionID = r.PartitionID
		result.Count = uint32(retCount)
		result.ExpireTime = expireTs
		result.ChannelName = r.ChannelName
		resp.SegIDAssignments = append(resp.SegIDAssignments, result)
	}
	return resp, nil
}

func (s *Server) openNewSegment(collectionID UniqueID, partitionID UniqueID, channelName string) error {
	group, err := s.insertChannelMgr.GetChannelGroup(collectionID, channelName)
	if err != nil {
		return err
	}
	segmentInfo, err := s.meta.BuildSegment(collectionID, partitionID, group)
	if err != nil {
		return err
	}
	if err = s.meta.AddSegment(segmentInfo); err != nil {
		return err
	}
	if err = s.segAllocator.OpenSegment(collectionID, partitionID, segmentInfo.SegmentID, segmentInfo.InsertChannels); err != nil {
		return err
	}
	return nil
}

func (s *Server) ShowSegments(req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	ids := s.meta.GetSegmentsByCollectionAndPartitionID(req.CollectionID, req.PartitionID)
	return &datapb.ShowSegmentResponse{SegmentIDs: ids}, nil
}

func (s *Server) GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	resp := &datapb.SegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}

	segmentInfo, err := s.meta.GetSegment(req.SegmentID)
	if err != nil {
		resp.Status.Reason = "get segment states error: " + err.Error()
		return resp, nil
	}
	resp.State = segmentInfo.State
	resp.CreateTime = segmentInfo.OpenTime
	resp.SealedTime = segmentInfo.SealedTime
	resp.FlushedTime = segmentInfo.FlushedTime
	// TODO start/end positions
	return resp, nil
}

func (s *Server) GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	panic("implement me")
}

func (s *Server) GetInsertChannels(req *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	resp := &internalpb2.StringList{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	contains, ret := s.insertChannelMgr.ContainsCollection(req.CollectionID)
	if contains {
		resp.Values = ret
		return resp, nil
	}
	channelGroups, err := s.insertChannelMgr.AllocChannels(req.CollectionID, len(s.nodes))
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	channels := make([]string, Params.InsertChannelNumPerCollection)
	for _, group := range channelGroups {
		for _, c := range group {
			channels = append(channels, c)
		}
	}
	// todo datanode watch dm channels
	resp.Values = channels
	return resp, nil
}

func (s *Server) GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	// todo implement
	return nil, nil
}

func (s *Server) GetPartitionStatistics(req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	// todo implement
	return nil, nil
}
