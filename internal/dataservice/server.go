// Copyright (C) 2019-2020 Zilliz. All rights reserved.//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package dataservice

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/logutil"

	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const role = "dataservice"

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)
type Server struct {
	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel context.CancelFunc
	serverLoopWg     sync.WaitGroup
	state            atomic.Value
	kvClient         *etcdkv.EtcdKV
	meta             *meta
	segAllocator     segmentAllocatorInterface
	statsHandler     *statsHandler
	allocator        allocatorInterface
	cluster          *dataNodeCluster
	masterClient     types.MasterService
	ddChannelMu      struct {
		sync.Mutex
		name string
	}
	session              *sessionutil.Session
	flushMsgStream       msgstream.MsgStream
	insertChannels       []string
	msFactory            msgstream.Factory
	createDataNodeClient func(addr string, serverID int64) (types.DataNode, error)
}

func CreateServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		ctx:       ctx,
		cluster:   newDataNodeCluster(),
		msFactory: factory,
	}
	s.insertChannels = s.getInsertChannels()
	s.createDataNodeClient = func(addr string, serverID int64) (types.DataNode, error) {
		node, err := grpcdatanodeclient.NewClient(addr, serverID, []string{Params.EtcdAddress}, 10)
		if err != nil {
			return nil, err
		}
		return node, nil
	}
	s.UpdateStateCode(internalpb.StateCode_Abnormal)
	return s, nil
}

func (s *Server) getInsertChannels() []string {
	channels := make([]string, 0, Params.InsertChannelNum)
	var i int64 = 0
	for ; i < Params.InsertChannelNum; i++ {
		channels = append(channels, Params.InsertChannelPrefixName+strconv.FormatInt(i, 10))
	}
	return channels
}

func (s *Server) SetMasterClient(masterClient types.MasterService) {
	s.masterClient = masterClient
}

// Register register data service at etcd
func (s *Server) Register() error {
	s.session = sessionutil.NewSession(s.ctx, []string{Params.EtcdAddress})
	s.session.Init(typeutil.DataServiceRole, Params.IP, true)
	Params.NodeID = s.session.ServerID
	return nil
}

func (s *Server) Init() error {
	return nil
}

func (s *Server) Start() error {
	var err error
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = s.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	if err := s.initMeta(); err != nil {
		return err
	}

	s.allocator = newAllocator(s.masterClient)

	s.startSegmentAllocator()
	s.statsHandler = newStatsHandler(s.meta)
	if err = s.loadMetaFromMaster(); err != nil {
		return err
	}
	if err = s.initMsgProducer(); err != nil {
		return err
	}
	s.startServerLoop()
	s.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("start success")
	return nil
}

func (s *Server) startSegmentAllocator() {
	stream := s.initSegmentInfoChannel()
	helper := createNewSegmentHelper(stream)
	s.segAllocator = newSegmentAllocator(s.meta, s.allocator, withAllocHelper(helper))
}

func (s *Server) initSegmentInfoChannel() msgstream.MsgStream {
	segmentInfoStream, _ := s.msFactory.NewMsgStream(s.ctx)
	segmentInfoStream.AsProducer([]string{Params.SegmentInfoChannelName})
	log.Debug("dataservice AsProducer: " + Params.SegmentInfoChannelName)
	segmentInfoStream.Start()
	return segmentInfoStream
}

func (s *Server) UpdateStateCode(code internalpb.StateCode) {
	s.state.Store(code)
}

func (s *Server) checkStateIsHealthy() bool {
	return s.state.Load().(internalpb.StateCode) == internalpb.StateCode_Healthy
}

func (s *Server) initMeta() error {
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints: []string{Params.EtcdAddress},
		})
		if err != nil {
			return err
		}
		s.kvClient = etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		s.meta, err = newMeta(s.kvClient)
		if err != nil {
			return err
		}
		return nil
	}
	return retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
}

func (s *Server) initMsgProducer() error {
	var err error
	// segment flush stream
	s.flushMsgStream, err = s.msFactory.NewMsgStream(s.ctx)
	if err != nil {
		return err
	}
	s.flushMsgStream.AsProducer([]string{Params.SegmentInfoChannelName})
	log.Debug("dataservice AsProducer:" + Params.SegmentInfoChannelName)
	s.flushMsgStream.Start()

	return nil
}

func (s *Server) loadMetaFromMaster() error {
	ctx := context.Background()
	log.Debug("loading collection meta from master")
	var err error
	if err = s.checkMasterIsHealthy(); err != nil {
		return err
	}
	if err = s.getDDChannel(); err != nil {
		return err
	}
	collections, err := s.masterClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     -1, // todo add msg id
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		DbName: "",
	})
	if err = VerifyResponse(collections, err); err != nil {
		return err
	}
	for _, collectionName := range collections.CollectionNames {
		collection, err := s.masterClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     -1, // todo
				Timestamp: 0,  // todo
				SourceID:  Params.NodeID,
			},
			DbName:         "",
			CollectionName: collectionName,
		})
		if err = VerifyResponse(collection, err); err != nil {
			log.Error("describe collection error", zap.String("collectionName", collectionName), zap.Error(err))
			continue
		}
		partitions, err := s.masterClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     -1, // todo
				Timestamp: 0,  // todo
				SourceID:  Params.NodeID,
			},
			DbName:         "",
			CollectionName: collectionName,
			CollectionID:   collection.CollectionID,
		})
		if err = VerifyResponse(partitions, err); err != nil {
			log.Error("show partitions error", zap.String("collectionName", collectionName), zap.Int64("collectionID", collection.CollectionID), zap.Error(err))
			continue
		}
		err = s.meta.AddCollection(&datapb.CollectionInfo{
			ID:         collection.CollectionID,
			Schema:     collection.Schema,
			Partitions: partitions.PartitionIDs,
		})
		if err != nil {
			log.Error("add collection to meta error", zap.Int64("collectionID", collection.CollectionID), zap.Error(err))
			continue
		}
	}
	log.Debug("load collection meta from master complete")
	return nil
}

func (s *Server) getDDChannel() error {
	s.ddChannelMu.Lock()
	defer s.ddChannelMu.Unlock()
	if len(s.ddChannelMu.name) == 0 {
		resp, err := s.masterClient.GetDdChannel(s.ctx)
		if err = VerifyResponse(resp, err); err != nil {
			return err
		}
		s.ddChannelMu.name = resp.Value
	}
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
		var resp *internalpb.ComponentStates
		var err error
		select {
		case <-ctx.Done():
			return errors.New("master is not healthy")
		case <-ticker.C:
			resp, err = s.masterClient.GetComponentStates(ctx)
			if err = VerifyResponse(resp, err); err != nil {
				return err
			}
		}
		if resp.State.StateCode == internalpb.StateCode_Healthy {
			break
		}
	}
	return nil
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(2)
	go s.startStatsChannel(s.serverLoopCtx)
	go s.startDataNodeTtLoop(s.serverLoopCtx)
}

func (s *Server) startStatsChannel(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	statsStream, _ := s.msFactory.NewMsgStream(ctx)
	statsStream.AsConsumer([]string{Params.StatisticsChannelName}, Params.DataServiceSubscriptionName)
	log.Debug("dataservice AsConsumer: " + Params.StatisticsChannelName + " : " + Params.DataServiceSubscriptionName)
	// try to restore last processed pos
	pos, err := s.loadStreamLastPos(streamTypeStats)
	log.Debug("load last pos of stats channel", zap.Any("pos", pos))
	if err == nil {
		err = statsStream.Seek(pos)
		if err != nil {
			log.Error("Failed to seek to last pos for statsStream",
				zap.String("StatisChanName", Params.StatisticsChannelName),
				zap.String("DataServiceSubscriptionName", Params.DataServiceSubscriptionName),
				zap.Error(err))
		}
	}
	statsStream.Start()
	defer statsStream.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgPack := statsStream.Consume()
		if msgPack == nil {
			return
		}
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_SegmentStatistics {
				log.Warn("receive unknown msg from segment statistics channel", zap.Stringer("msgType", msg.Type()))
				continue
			}
			ssMsg := msg.(*msgstream.SegmentStatisticsMsg)
			for _, stat := range ssMsg.SegStats {
				if err := s.statsHandler.HandleSegmentStat(stat); err != nil {
					log.Error("handle segment stat error", zap.Int64("segmentID", stat.SegmentID), zap.Error(err))
					continue
				}
			}
			if ssMsg.MsgPosition != nil {
				err := s.storeStreamPos(streamTypeStats, ssMsg.MsgPosition)
				if err != nil {
					log.Error("Fail to store current success pos for Stats stream",
						zap.Stringer("pos", ssMsg.MsgPosition),
						zap.Error(err))
				}
			} else {
				log.Warn("Empty Msg Pos found ", zap.Int64("msgid", msg.ID()))
			}
		}
	}
}

func (s *Server) startDataNodeTtLoop(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	ttMsgStream, err := s.msFactory.NewMsgStream(ctx)
	if err != nil {
		log.Error("new msg stream failed", zap.Error(err))
		return
	}
	ttMsgStream.AsConsumer([]string{Params.TimeTickChannelName},
		Params.DataServiceSubscriptionName)
	log.Debug(fmt.Sprintf("dataservice AsConsumer:%s:%s",
		Params.TimeTickChannelName, Params.DataServiceSubscriptionName))
	ttMsgStream.Start()
	defer ttMsgStream.Close()
	for {
		select {
		case <-ctx.Done():
			log.Debug("data node tt loop done")
			return
		default:
		}
		msgPack := ttMsgStream.Consume()
		if msgPack == nil {
			return
		}
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_DataNodeTt {
				log.Warn("receive unexpected msg type from tt channel",
					zap.Stringer("msgType", msg.Type()))
				continue
			}
			ttMsg := msg.(*msgstream.DataNodeTtMsg)

			coll2Segs := make(map[UniqueID][]UniqueID)
			ch := ttMsg.ChannelName
			ts := ttMsg.Timestamp
			segments, err := s.segAllocator.GetFlushableSegments(ctx, ch, ts)
			if err != nil {
				log.Warn("get flushable segments failed", zap.Error(err))
				continue
			}
			for _, id := range segments {
				sInfo, err := s.meta.GetSegment(id)
				if err != nil {
					log.Error("get segment from meta error", zap.Int64("id", id),
						zap.Error(err))
					continue
				}
				collID, segID := sInfo.CollectionID, sInfo.ID
				coll2Segs[collID] = append(coll2Segs[collID], segID)
			}

			for collID, segIDs := range coll2Segs {
				s.cluster.FlushSegment(&datapb.FlushSegmentsRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Flush,
						MsgID:     -1, // todo add msg id
						Timestamp: 0,  // todo
						SourceID:  Params.NodeID,
					},
					CollectionID: collID,
					SegmentIDs:   segIDs,
				})
			}
		}
	}
}

func (s *Server) Stop() error {
	s.cluster.ShutDownClients()
	s.flushMsgStream.Close()
	s.stopServerLoop()
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	return s.kvClient.RemoveWithPrefix("")
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) newDataNode(ip string, port int64, id UniqueID) (*dataNode, error) {
	client, err := s.createDataNodeClient(fmt.Sprintf("%s:%d", ip, port), id)
	if err != nil {
		return nil, err
	}
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

//func (s *Server) validateAllocRequest(collID UniqueID, partID UniqueID, channelName string) error {
//	if !s.meta.HasCollection(collID) {
//		return fmt.Errorf("can not find collection %d", collID)
//	}
//	if !s.meta.HasPartition(collID, partID) {
//		return fmt.Errorf("can not find partition %d", partID)
//	}
//	for _, name := range s.insertChannels {
//		if name == channelName {
//			return nil
//		}
//	}
//	return fmt.Errorf("can not find channel %s", channelName)
//}

func (s *Server) loadCollectionFromMaster(ctx context.Context, collectionID int64) error {
	resp, err := s.masterClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_DescribeCollection,
			SourceID: Params.NodeID,
		},
		DbName:       "",
		CollectionID: collectionID,
	})
	if err = VerifyResponse(resp, err); err != nil {
		return err
	}
	presp, err := s.masterClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     -1, // todo
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		DbName:         "",
		CollectionName: resp.Schema.Name,
		CollectionID:   resp.CollectionID,
	})
	if err = VerifyResponse(presp, err); err != nil {
		log.Error("show partitions error", zap.String("collectionName", resp.Schema.Name), zap.Int64("collectionID", resp.CollectionID), zap.Error(err))
		return err
	}
	collInfo := &datapb.CollectionInfo{
		ID:         resp.CollectionID,
		Schema:     resp.Schema,
		Partitions: presp.PartitionIDs,
	}
	return s.meta.AddCollection(collInfo)
}

func (s *Server) prepareBinlogAndPos(req *datapb.SaveBinlogPathsRequest) (map[string]string, error) {
	meta := make(map[string]string)
	segInfo, err := s.meta.GetSegment(req.GetSegmentID())
	if err != nil {
		log.Error("Failed to get segment info", zap.Int64("segmentID", req.GetSegmentID()), zap.Error(err))
		return nil, err
	}
	log.Debug("segment", zap.Int64("segment", segInfo.CollectionID))

	fieldMeta, err := s.prepareField2PathMeta(req.SegmentID, req.Field2BinlogPaths)
	if err != nil {
		return nil, err
	}
	for k, v := range fieldMeta {
		meta[k] = v
	}
	ddlMeta, err := s.prepareDDLBinlogMeta(req.CollectionID, req.GetDdlBinlogPaths())
	if err != nil {
		return nil, err
	}
	for k, v := range ddlMeta {
		meta[k] = v
	}
	segmentPos, err := s.prepareSegmentPos(segInfo, req.GetDmlPosition(), req.GetDdlPosition())
	if err != nil {
		return nil, err
	}
	for k, v := range segmentPos {
		meta[k] = v
	}

	return meta, nil
}

func composeSegmentFlushMsgPack(segmentID UniqueID) msgstream.MsgPack {
	msgPack := msgstream.MsgPack{
		Msgs: make([]msgstream.TsMsg, 0, 1),
	}
	completeFlushMsg := internalpb.SegmentFlushCompletedMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentFlushDone,
			MsgID:     0, // TODO
			Timestamp: 0, // TODO
			SourceID:  Params.NodeID,
		},
		SegmentID: segmentID,
	}
	var msg msgstream.TsMsg = &msgstream.FlushCompletedMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SegmentFlushCompletedMsg: completeFlushMsg,
	}

	msgPack.Msgs = append(msgPack.Msgs, msg)
	return msgPack
}
