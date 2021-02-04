package dataservice

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/distributed/datanode"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/timesync"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const role = "dataservice"

type DataService interface {
	typeutil.Service
	typeutil.Component
	RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
	Flush(req *datapb.FlushRequest) (*commonpb.Status, error)

	AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error)
	ShowSegments(req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error)
	GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
	GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error)
	GetSegmentInfoChannel() (*milvuspb.StringResponse, error)
	GetInsertChannels(req *datapb.InsertChannelRequest) (*internalpb2.StringList, error)
	GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error)
	GetPartitionStatistics(req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error)
	GetComponentStates() (*internalpb2.ComponentStates, error)
	GetCount(req *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error)
	GetSegmentInfo(req *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error)
}

type MasterClient interface {
	ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
	DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
	GetDdChannel() (string, error)
	AllocTimestamp(in *masterpb.TsoRequest) (*masterpb.TsoResponse, error)
	AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error)
	GetComponentStates() (*internalpb2.ComponentStates, error)
}

type DataNodeClient interface {
	WatchDmChannels(in *datapb.WatchDmChannelRequest) (*commonpb.Status, error)
	GetComponentStates(empty *commonpb.Empty) (*internalpb2.ComponentStates, error)
	FlushSegments(in *datapb.FlushSegRequest) (*commonpb.Status, error)
	Stop() error
}

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
	Server    struct {
		ctx               context.Context
		serverLoopCtx     context.Context
		serverLoopCancel  context.CancelFunc
		serverLoopWg      sync.WaitGroup
		state             atomic.Value
		client            *etcdkv.EtcdKV
		meta              *meta
		segAllocator      segmentAllocator
		statsHandler      *statsHandler
		ddHandler         *ddHandler
		allocator         allocator
		cluster           *dataNodeCluster
		msgProducer       *timesync.MsgProducer
		registerFinishCh  chan struct{}
		masterClient      MasterClient
		ttMsgStream       msgstream.MsgStream
		k2sMsgStream      msgstream.MsgStream
		ddChannelName     string
		segmentInfoStream msgstream.MsgStream
		insertChannels    []string
		ttBarrier         timesync.TimeTickBarrier
	}
)

func CreateServer(ctx context.Context) (*Server, error) {
	Params.Init()
	ch := make(chan struct{})
	s := &Server{
		ctx:              ctx,
		registerFinishCh: ch,
		cluster:          newDataNodeCluster(ch),
	}
	s.insertChannels = s.getInsertChannels()
	s.state.Store(internalpb2.StateCode_INITIALIZING)
	return s, nil
}

func (s *Server) getInsertChannels() []string {
	channels := make([]string, Params.InsertChannelNum)
	var i int64 = 0
	for ; i < Params.InsertChannelNum; i++ {
		channels[i] = Params.InsertChannelPrefixName + strconv.FormatInt(i, 10)
	}
	return channels
}

func (s *Server) SetMasterClient(masterClient MasterClient) {
	s.masterClient = masterClient
}

func (s *Server) Init() error {
	return nil
}

func (s *Server) Start() error {
	var err error
	s.allocator = newAllocatorImpl(s.masterClient)
	if err = s.initMeta(); err != nil {
		return err
	}
	s.statsHandler = newStatsHandler(s.meta)
	s.segAllocator = newSegmentAllocator(s.meta, s.allocator)
	s.ddHandler = newDDHandler(s.meta, s.segAllocator)
	s.initSegmentInfoChannel()
	if err = s.loadMetaFromMaster(); err != nil {
		return err
	}
	s.waitDataNodeRegister()
	s.cluster.WatchInsertChannels(s.insertChannels)
	s.startServerLoop()
	if err = s.initMsgProducer(); err != nil {
		return err
	}
	s.state.Store(internalpb2.StateCode_HEALTHY)
	log.Println("start success")
	return nil
}

func (s *Server) checkStateIsHealthy() bool {
	return s.state.Load().(internalpb2.StateCode) == internalpb2.StateCode_HEALTHY
}

func (s *Server) initMeta() error {
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})
	if err != nil {
		return err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	s.client = etcdKV
	s.meta, err = newMeta(etcdKV)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) initSegmentInfoChannel() {
	factory := pulsarms.NewFactory(Params.PulsarAddress, 1024, 1024)
	segmentInfoStream, _ := factory.NewMsgStream(s.ctx)
	segmentInfoStream.AsProducer([]string{Params.SegmentInfoChannelName})
	s.segmentInfoStream = segmentInfoStream
	s.segmentInfoStream.Start()
}
func (s *Server) initMsgProducer() error {
	var err error
	factory := pulsarms.NewFactory(Params.PulsarAddress, 1024, 1024)
	if s.ttMsgStream, err = factory.NewMsgStream(s.ctx); err != nil {
		return err
	}
	s.ttMsgStream.AsConsumer([]string{Params.TimeTickChannelName}, Params.DataServiceSubscriptionName)
	s.ttMsgStream.Start()
	if s.k2sMsgStream, err = factory.NewMsgStream(s.ctx); err != nil {
		return err
	}
	s.k2sMsgStream.AsProducer(Params.K2SChannelNames)
	s.k2sMsgStream.Start()
	dataNodeTTWatcher := newDataNodeTimeTickWatcher(s.meta, s.segAllocator, s.cluster)
	k2sMsgWatcher := timesync.NewMsgTimeTickWatcher(s.k2sMsgStream)
	if s.msgProducer, err = timesync.NewTimeSyncMsgProducer(s.ttBarrier, dataNodeTTWatcher, k2sMsgWatcher); err != nil {
		return err
	}
	s.msgProducer.Start(s.ctx)
	return nil
}

func (s *Server) loadMetaFromMaster() error {
	log.Println("loading collection meta from master")
	if err := s.checkMasterIsHealthy(); err != nil {
		return err
	}
	if s.ddChannelName == "" {
		channel, err := s.masterClient.GetDdChannel()
		if err != nil {
			return err
		}
		s.ddChannelName = channel
	}
	collections, err := s.masterClient.ShowCollections(&milvuspb.ShowCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kShowCollections,
			MsgID:     -1, // todo add msg id
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		DbName: "",
	})
	if err != nil {
		return err
	}
	for _, collectionName := range collections.CollectionNames {
		collection, err := s.masterClient.DescribeCollection(&milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDescribeCollection,
				MsgID:     -1, // todo
				Timestamp: 0,  // todo
				SourceID:  Params.NodeID,
			},
			DbName:         "",
			CollectionName: collectionName,
		})
		if err != nil {
			log.Println(err.Error())
			continue
		}
		partitions, err := s.masterClient.ShowPartitions(&milvuspb.ShowPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowPartitions,
				MsgID:     -1, // todo
				Timestamp: 0,  // todo
				SourceID:  Params.NodeID,
			},
			DbName:         "",
			CollectionName: collectionName,
			CollectionID:   collection.CollectionID,
		})
		if err != nil {
			log.Println(err.Error())
			continue
		}
		err = s.meta.AddCollection(&collectionInfo{
			ID:         collection.CollectionID,
			Schema:     collection.Schema,
			Partitions: partitions.PartitionIDs,
		})
		if err != nil {
			log.Println(err.Error())
			continue
		}
	}
	log.Println("load collection meta from master complete")
	return nil
}

func (s *Server) checkMasterIsHealthy() error {
	ticker := time.NewTicker(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer func() {
		ticker.Stop()
		cancel()
	}()
	for {
		var resp *internalpb2.ComponentStates
		var err error
		select {
		case <-ctx.Done():
			return fmt.Errorf("master is not healthy")
		case <-ticker.C:
			resp, err = s.masterClient.GetComponentStates()
			if err != nil {
				return err
			}
			if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
				return errors.New(resp.Status.Reason)
			}
		}
		if resp.State.StateCode == internalpb2.StateCode_HEALTHY {
			break
		}
	}
	return nil
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(4)
	go s.startStatsChannel(s.serverLoopCtx)
	go s.startSegmentFlushChannel(s.serverLoopCtx)
	go s.startDDChannel(s.serverLoopCtx)
	go s.startTTBarrier(s.serverLoopCtx)
}

func (s *Server) startStatsChannel(ctx context.Context) {
	defer s.serverLoopWg.Done()
	factory := pulsarms.NewFactory(Params.PulsarAddress, 1024, 1024)
	statsStream, _ := factory.NewMsgStream(ctx)
	statsStream.AsConsumer([]string{Params.StatisticsChannelName}, Params.DataServiceSubscriptionName)
	statsStream.Start()
	defer statsStream.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgPack := statsStream.Consume()
		for _, msg := range msgPack.Msgs {
			statistics := msg.(*msgstream.SegmentStatisticsMsg)
			for _, stat := range statistics.SegStats {
				if err := s.statsHandler.HandleSegmentStat(stat); err != nil {
					log.Println(err.Error())
					continue
				}
			}
		}
	}
}

func (s *Server) startSegmentFlushChannel(ctx context.Context) {
	defer s.serverLoopWg.Done()
	factory := pulsarms.NewFactory(Params.PulsarAddress, 1024, 1024)
	flushStream, _ := factory.NewMsgStream(ctx)
	flushStream.AsConsumer([]string{Params.SegmentInfoChannelName}, Params.DataServiceSubscriptionName)
	flushStream.Start()
	defer flushStream.Close()
	for {
		select {
		case <-ctx.Done():
			log.Println("segment flush channel shut down")
			return
		default:
		}
		msgPack := flushStream.Consume()
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_kSegmentFlushDone {
				continue
			}
			realMsg := msg.(*msgstream.FlushCompletedMsg)

			segmentInfo, err := s.meta.GetSegment(realMsg.SegmentID)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			segmentInfo.FlushedTime = realMsg.BeginTimestamp
			if err = s.meta.UpdateSegment(segmentInfo); err != nil {
				log.Println(err.Error())
				continue
			}
		}
	}
}

func (s *Server) startDDChannel(ctx context.Context) {
	defer s.serverLoopWg.Done()
	factory := pulsarms.NewFactory(Params.PulsarAddress, 1024, 1024)
	ddStream, _ := factory.NewMsgStream(ctx)
	ddStream.AsConsumer([]string{s.ddChannelName}, Params.DataServiceSubscriptionName)
	ddStream.Start()
	defer ddStream.Close()
	for {
		select {
		case <-ctx.Done():
			log.Println("dd channel shut down")
			return
		default:
		}
		msgPack := ddStream.Consume()
		for _, msg := range msgPack.Msgs {
			if err := s.ddHandler.HandleDDMsg(msg); err != nil {
				log.Println(err.Error())
				continue
			}
		}
	}
}

func (s *Server) startTTBarrier(ctx context.Context) {
	defer s.serverLoopWg.Done()
	s.ttBarrier = timesync.NewHardTimeTickBarrier(ctx, s.ttMsgStream, s.cluster.GetNodeIDs())
	s.ttBarrier.StartBackgroundLoop()
}

func (s *Server) waitDataNodeRegister() {
	log.Println("waiting data node to register")
	<-s.registerFinishCh
	log.Println("all data nodes register")
}

func (s *Server) Stop() error {
	s.cluster.ShutDownClients()
	s.ttMsgStream.Close()
	s.k2sMsgStream.Close()
	s.msgProducer.Close()
	s.segmentInfoStream.Close()
	s.stopServerLoop()
	return nil
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) GetComponentStates() (*internalpb2.ComponentStates, error) {
	resp := &internalpb2.ComponentStates{
		State: &internalpb2.ComponentInfo{
			NodeID:    Params.NodeID,
			Role:      role,
			StateCode: s.state.Load().(internalpb2.StateCode),
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	dataNodeStates, err := s.cluster.GetDataNodeStates()
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.SubcomponentStates = dataNodeStates
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	return resp, nil
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
	ret := &datapb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	node, err := s.newDataNode(req.Address.Ip, req.Address.Port, req.Base.SourceID)
	if err != nil {
		return nil, err
	}
	s.cluster.Register(node)
	if s.ddChannelName == "" {
		resp, err := s.masterClient.GetDdChannel()
		if err != nil {
			ret.Status.Reason = err.Error()
			return ret, err
		}
		s.ddChannelName = resp
	}
	ret.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	ret.InitParams = &internalpb2.InitParams{
		NodeID: Params.NodeID,
		StartParams: []*commonpb.KeyValuePair{
			{Key: "DDChannelName", Value: s.ddChannelName},
			{Key: "SegmentStatisticsChannelName", Value: Params.StatisticsChannelName},
			{Key: "TimeTickChannelName", Value: Params.TimeTickChannelName},
			{Key: "CompleteFlushChannelName", Value: Params.SegmentInfoChannelName},
		},
	}
	return ret, nil
}

func (s *Server) newDataNode(ip string, port int64, id UniqueID) (*dataNode, error) {
	client := datanode.NewClient(fmt.Sprintf("%s:%d", ip, port))
	if err := client.Init(); err != nil {
		return nil, err
	}
	if err := client.Start(); err != nil {
		return nil, err
	}
	return &dataNode{
		id: id,
		address: struct {
			ip   string
			port int64
		}{ip: ip, port: port},
		client:     client,
		channelNum: 0,
	}, nil
}

func (s *Server) Flush(req *datapb.FlushRequest) (*commonpb.Status, error) {
	if !s.checkStateIsHealthy() {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    "server is initializing",
		}, nil
	}
	s.segAllocator.SealAllSegments(req.CollectionID)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (s *Server) AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	resp := &datapb.AssignSegIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		SegIDAssignments: make([]*datapb.SegIDAssignment, 0),
	}
	if !s.checkStateIsHealthy() {
		resp.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		resp.Status.Reason = "server is initializing"
		return resp, nil
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
	id, err := s.allocator.allocID()
	if err != nil {
		return err
	}
	segmentInfo, err := BuildSegment(collectionID, partitionID, id, channelName)
	if err != nil {
		return err
	}
	if err = s.meta.AddSegment(segmentInfo); err != nil {
		return err
	}
	if err = s.segAllocator.OpenSegment(segmentInfo); err != nil {
		return err
	}
	infoMsg := &msgstream.SegmentInfoMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SegmentMsg: datapb.SegmentMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kSegmentInfo,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.NodeID,
			},
			Segment: segmentInfo,
		},
	}
	msgPack := &pulsarms.MsgPack{
		Msgs: []msgstream.TsMsg{infoMsg},
	}
	if err = s.segmentInfoStream.Produce(msgPack); err != nil {
		return err
	}
	return nil
}

func (s *Server) ShowSegments(req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	resp := &datapb.ShowSegmentResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	if !s.checkStateIsHealthy() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}
	ids := s.meta.GetSegmentsOfPartition(req.CollectionID, req.PartitionID)
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	resp.SegmentIDs = ids
	return resp, nil
}

func (s *Server) GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	resp := &datapb.SegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	if !s.checkStateIsHealthy() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}

	for _, segmentID := range req.SegmentIDs {
		state := &datapb.SegmentStateInfo{
			Status:    &commonpb.Status{},
			SegmentID: segmentID,
		}
		segmentInfo, err := s.meta.GetSegment(segmentID)
		if err != nil {
			state.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
			state.Status.Reason = "get segment states error: " + err.Error()
		} else {
			state.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
			state.State = segmentInfo.State
			state.CreateTime = segmentInfo.OpenTime
			state.SealedTime = segmentInfo.SealedTime
			state.FlushedTime = segmentInfo.FlushedTime
			state.StartPosition = segmentInfo.StartPosition
			state.EndPosition = segmentInfo.EndPosition
		}
		resp.States = append(resp.States, state)
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS

	return resp, nil
}

func (s *Server) GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	resp := &datapb.InsertBinlogPathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	p := path.Join(Params.SegmentFlushMetaPath, strconv.FormatInt(req.SegmentID, 10))
	value, err := s.client.Load(p)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	flushMeta := &datapb.SegmentFlushMeta{}
	err = proto.UnmarshalText(value, flushMeta)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	fields := make([]UniqueID, len(flushMeta.Fields))
	paths := make([]*internalpb2.StringList, len(flushMeta.Fields))
	for i, field := range flushMeta.Fields {
		fields[i] = field.FieldID
		paths[i] = &internalpb2.StringList{Values: field.BinlogPaths}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	resp.FieldIDs = fields
	resp.Paths = paths
	return resp, nil
}

func (s *Server) GetInsertChannels(req *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return &internalpb2.StringList{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Values: s.insertChannels,
	}, nil
}

func (s *Server) GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	resp := &datapb.CollectionStatsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	nums, err := s.meta.GetNumRowsOfCollection(req.CollectionID)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	memsize, err := s.meta.GetMemSizeOfCollection(req.CollectionID)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "nums", Value: strconv.FormatInt(nums, 10)})
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "memsize", Value: strconv.FormatInt(memsize, 10)})
	return resp, nil
}

func (s *Server) GetPartitionStatistics(req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	// todo implement
	return nil, nil
}

func (s *Server) GetSegmentInfoChannel() (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Value: Params.SegmentInfoChannelName,
	}, nil
}

func (s *Server) GetCount(req *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error) {
	resp := &datapb.CollectionCountResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	if !s.checkStateIsHealthy() {
		resp.Status.Reason = "data service is not healthy"
		return resp, nil
	}
	nums, err := s.meta.GetNumRowsOfCollection(req.CollectionID)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Count = nums
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	return resp, nil
}

func (s *Server) GetSegmentInfo(req *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error) {
	resp := &datapb.SegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	if !s.checkStateIsHealthy() {
		resp.Status.Reason = "data service is not healthy"
		return resp, nil
	}
	infos := make([]*datapb.SegmentInfo, len(req.SegmentIDs))
	for i, id := range req.SegmentIDs {
		segmentInfo, err := s.meta.GetSegment(id)
		if err != nil {
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		infos[i] = segmentInfo
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	resp.Infos = infos
	return resp, nil
}
