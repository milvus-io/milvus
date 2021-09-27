// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	rootcoordclient "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/logutil"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const connEtcdMaxRetryTime = 100000

var (
	// TODO: sunby put to config
	enableTtChecker  = true
	ttCheckerName    = "dataTtChecker"
	ttMaxInterval    = 3 * time.Minute
	ttCheckerWarnMsg = fmt.Sprintf("we haven't received tt for %f minutes", ttMaxInterval.Minutes())
)

type (
	// UniqueID shortcut for typeutil.UniqueID
	UniqueID = typeutil.UniqueID
	// Timestamp shortcurt for typeutil.Timestamp
	Timestamp = typeutil.Timestamp
)

// ServerState type alias, presents datacoord Server State
type ServerState = int64

const (
	// ServerStateStopped state stands for just created or stopped `Server` instance
	ServerStateStopped ServerState = 0
	// ServerStateInitializing state stands initializing `Server` instance
	ServerStateInitializing ServerState = 1
	// ServerStateHealthy state stands for healthy `Server` instance
	ServerStateHealthy ServerState = 2
)

// DataNodeCreatorFunc creator function for datanode
type DataNodeCreatorFunc func(ctx context.Context, addr string) (types.DataNode, error)

// RootCoordCreatorFunc creator function for rootcoord
type RootCoordCreatorFunc func(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error)

// Server implements `types.Datacoord`
// handles Data Cooridinator related jobs
type Server struct {
	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel context.CancelFunc
	serverLoopWg     sync.WaitGroup
	isServing        ServerState
	helper           ServerHelper

	kvClient        *etcdkv.EtcdKV
	meta            *meta
	segmentManager  Manager
	allocator       allocator
	cluster         *Cluster
	rootCoordClient types.RootCoord

	metricsCacheManager *metricsinfo.MetricsCacheManager

	flushCh   chan UniqueID
	msFactory msgstream.Factory

	session  *sessionutil.Session
	activeCh <-chan bool
	eventCh  <-chan *sessionutil.SessionEvent

	dataNodeCreator        DataNodeCreatorFunc
	rootCoordClientCreator RootCoordCreatorFunc
}

// ServerHelper datacoord server injection helper
type ServerHelper struct {
	eventAfterHandleDataNodeTt func()
}

func defaultServerHelper() ServerHelper {
	return ServerHelper{
		eventAfterHandleDataNodeTt: func() {},
	}
}

// Option utility function signature to set DataCoord server attributes
type Option func(svr *Server)

// SetRootCoordCreator returns an `Option` setting RootCoord creator with provided parameter
func SetRootCoordCreator(creator RootCoordCreatorFunc) Option {
	return func(svr *Server) {
		svr.rootCoordClientCreator = creator
	}
}

// SetServerHelper returns an `Option` setting ServerHelp with provided parameter
func SetServerHelper(helper ServerHelper) Option {
	return func(svr *Server) {
		svr.helper = helper
	}
}

// SetCluster returns an `Option` setting Cluster with provided parameter
func SetCluster(cluster *Cluster) Option {
	return func(svr *Server) {
		svr.cluster = cluster
	}
}

// SetDataNodeCreator returns an `Option` setting DataNode create function
func SetDataNodeCreator(creator DataNodeCreatorFunc) Option {
	return func(svr *Server) {
		svr.dataNodeCreator = creator
	}
}

// CreateServer create `Server` instance
func CreateServer(ctx context.Context, factory msgstream.Factory, opts ...Option) (*Server, error) {
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		ctx:                    ctx,
		msFactory:              factory,
		flushCh:                make(chan UniqueID, 1024),
		dataNodeCreator:        defaultDataNodeCreatorFunc,
		rootCoordClientCreator: defaultRootCoordCreatorFunc,
		helper:                 defaultServerHelper(),

		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
	}

	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// defaultDataNodeCreatorFunc defines the default behavior to get a DataNode
func defaultDataNodeCreatorFunc(ctx context.Context, addr string) (types.DataNode, error) {
	return datanodeclient.NewClient(ctx, addr)
}

func defaultRootCoordCreatorFunc(ctx context.Context, metaRootPath string, etcdEndpoints []string) (types.RootCoord, error) {
	return rootcoordclient.NewClient(ctx, metaRootPath, etcdEndpoints)
}

// Register register data service at etcd
func (s *Server) Register() error {
	s.session = sessionutil.NewSession(s.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	if s.session == nil {
		return errors.New("failed to initialize session")
	}
	s.activeCh = s.session.Init(typeutil.DataCoordRole, Params.IP, true)
	Params.NodeID = s.session.ServerID
	return nil
}

// Init change server state to Initializing
func (s *Server) Init() error {
	atomic.StoreInt64(&s.isServing, ServerStateInitializing)
	return nil
}

// Start initialize `Server` members and start loops, follow steps are taken:
// 1. initialize message factory parameters
// 2. initialize root coord client, meta, datanode cluster, segment info channel,
//		allocator, segment manager
// 3. start service discovery and server loops, which includes message stream handler (segment statistics,datanode tt)
//		datanodes etcd watch, etcd alive check and flush completed status check
// 4. set server state to Healthy
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
	if err = s.initRootCoordClient(); err != nil {
		return err
	}

	if err = s.initMeta(); err != nil {
		return err
	}

	if err = s.initCluster(); err != nil {
		return err
	}

	s.allocator = newRootCoordAllocator(s.rootCoordClient)

	s.startSegmentManager()
	if err = s.initServiceDiscovery(); err != nil {
		return err
	}

	s.startServerLoop()

	helper := NewMoveBinlogPathHelper(s.kvClient, s.meta)
	if err := helper.Execute(); err != nil {
		return err
	}

	Params.CreatedTime = time.Now()
	Params.UpdatedTime = time.Now()

	atomic.StoreInt64(&s.isServing, ServerStateHealthy)
	log.Debug("dataCoordinator startup success")

	return nil
}

func (s *Server) initCluster() error {
	var err error
	// cluster could be set by options
	// by-pass default NewCluster process if already set
	if s.cluster == nil {
		s.cluster, err = NewCluster(s.ctx, s.kvClient, NewNodesInfo(), s)
	}
	return err
}

func (s *Server) initServiceDiscovery() error {
	sessions, rev, err := s.session.GetSessions(typeutil.DataNodeRole)
	if err != nil {
		log.Debug("dataCoord initMeta failed", zap.Error(err))
		return err
	}
	log.Debug("registered sessions", zap.Any("sessions", sessions))

	datanodes := make([]*NodeInfo, 0, len(sessions))
	for _, session := range sessions {
		info := &datapb.DataNodeInfo{
			Address:  session.Address,
			Version:  session.ServerID,
			Channels: []*datapb.ChannelStatus{},
		}
		nodeInfo := NewNodeInfo(s.ctx, info)
		datanodes = append(datanodes, nodeInfo)
	}

	s.cluster.Startup(datanodes)

	s.eventCh = s.session.WatchServices(typeutil.DataNodeRole, rev+1)
	return nil
}

func (s *Server) startSegmentManager() {
	s.segmentManager = newSegmentManager(s.meta, s.allocator)
}

func (s *Server) initMeta() error {
	connectEtcdFn := func() error {
		etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		if err != nil {
			return err
		}

		s.kvClient = etcdKV
		s.meta, err = NewMeta(s.kvClient)
		if err != nil {
			return err
		}
		return nil
	}
	return retry.Do(s.ctx, connectEtcdFn, retry.Attempts(connEtcdMaxRetryTime))
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(5)
	go s.startStatsChannel(s.serverLoopCtx)
	go s.startDataNodeTtLoop(s.serverLoopCtx)
	go s.startWatchService(s.serverLoopCtx)
	go s.startActiveCheck(s.serverLoopCtx)
	go s.startFlushLoop(s.serverLoopCtx)
}

func (s *Server) startStatsChannel(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	statsStream, _ := s.msFactory.NewMsgStream(ctx)
	statsStream.AsConsumer([]string{Params.StatisticsChannelName}, Params.DataCoordSubscriptionName)
	log.Debug("dataCoord create stats channel consumer",
		zap.String("channelName", Params.StatisticsChannelName),
		zap.String("descriptionName", Params.DataCoordSubscriptionName))
	statsStream.Start()
	defer statsStream.Close()
	for {
		select {
		case <-ctx.Done():
			log.Debug("stats channel shutdown")
			return
		default:
		}
		msgPack := statsStream.Consume()
		if msgPack == nil {
			log.Debug("receive nil stats msg, shutdown stats channel")
			return
		}
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_SegmentStatistics {
				log.Warn("receive unknown msg from segment statistics channel",
					zap.Stringer("msgType", msg.Type()))
				continue
			}
			ssMsg := msg.(*msgstream.SegmentStatisticsMsg)
			for _, stat := range ssMsg.SegStats {
				s.meta.SetCurrentRows(stat.GetSegmentID(), stat.GetNumRows())
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
		Params.DataCoordSubscriptionName)
	log.Debug("dataCoord create time tick channel consumer",
		zap.String("timeTickChannelName", Params.TimeTickChannelName),
		zap.String("subscriptionName", Params.DataCoordSubscriptionName))
	ttMsgStream.Start()
	defer ttMsgStream.Close()

	var checker *LongTermChecker
	if enableTtChecker {
		checker = NewLongTermChecker(ctx, ttCheckerName, ttMaxInterval, ttCheckerWarnMsg)
		checker.Start()
		defer checker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			log.Debug("data node tt loop shutdown")
			return
		default:
		}
		msgPack := ttMsgStream.Consume()
		if msgPack == nil {
			log.Debug("receive nil tt msg, shutdown tt channel")
			return
		}
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_DataNodeTt {
				log.Warn("receive unexpected msg type from tt channel",
					zap.Stringer("msgType", msg.Type()))
				continue
			}
			ttMsg := msg.(*msgstream.DataNodeTtMsg)
			if enableTtChecker {
				checker.Check()
			}

			ch := ttMsg.ChannelName
			ts := ttMsg.Timestamp
			s.segmentManager.ExpireAllocations(ch, ts)
			segments, err := s.segmentManager.GetFlushableSegments(ctx, ch, ts)
			if err != nil {
				log.Warn("get flushable segments failed", zap.Error(err))
				continue
			}

			if len(segments) == 0 {
				continue
			}
			log.Debug("flush segments", zap.Int64s("segmentIDs", segments))
			segmentInfos := make([]*datapb.SegmentInfo, 0, len(segments))
			for _, id := range segments {
				sInfo := s.meta.GetSegment(id)
				if sInfo == nil {
					log.Error("get segment from meta error", zap.Int64("id", id),
						zap.Error(err))
					continue
				}
				segmentInfos = append(segmentInfos, sInfo.SegmentInfo)
				s.meta.SetLastFlushTime(id, time.Now())
			}
			if len(segmentInfos) > 0 {
				s.cluster.Flush(segmentInfos)
			}
		}
		s.helper.eventAfterHandleDataNodeTt()
	}
}

func (s *Server) startWatchService(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Debug("watch service shutdown")
			return
		case event := <-s.eventCh:
			s.handleSessionEvent(ctx, event)
		}
	}
}

// handles session events - DataNodes Add/Del
func (s *Server) handleSessionEvent(ctx context.Context, event *sessionutil.SessionEvent) {
	if event == nil {
		return
	}
	info := &datapb.DataNodeInfo{
		Address:  event.Session.Address,
		Version:  event.Session.ServerID,
		Channels: []*datapb.ChannelStatus{},
	}
	node := NewNodeInfo(ctx, info)
	switch event.EventType {
	case sessionutil.SessionAddEvent:
		log.Info("received datanode register",
			zap.String("address", info.Address),
			zap.Int64("serverID", info.Version))
		s.cluster.Register(node)
		s.metricsCacheManager.InvalidateSystemInfoMetrics()
	case sessionutil.SessionDelEvent:
		log.Info("received datanode unregister",
			zap.String("address", info.Address),
			zap.Int64("serverID", info.Version))
		s.cluster.UnRegister(node)
		s.metricsCacheManager.InvalidateSystemInfoMetrics()
	default:
		log.Warn("receive unknown service event type",
			zap.Any("type", event.EventType))
	}
}

func (s *Server) startActiveCheck(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		select {
		case _, ok := <-s.activeCh:
			if ok {
				continue
			}
			go func() { s.Stop() }()
			log.Debug("disconnect with etcd and shutdown data coordinator")
			return
		case <-ctx.Done():
			log.Debug("connection check shutdown")
			return
		}
	}
}

func (s *Server) startFlushLoop(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	// send `Flushing` segments
	go s.handleFlushingSegments(ctx2)
	for {
		select {
		case <-ctx.Done():
			log.Debug("flush loop shutdown")
			return
		case segmentID := <-s.flushCh:
			//Ignore return error
			_ = s.postFlush(ctx, segmentID)
		}
	}
}

// post function after flush is done
// 1. check segment id is valid
// 2. notify RootCoord segment is flushed
// 3. change segment state to `Flushed` in meta
func (s *Server) postFlush(ctx context.Context, segmentID UniqueID) error {
	segment := s.meta.GetSegment(segmentID)
	if segment == nil {
		log.Warn("failed to get flused segment", zap.Int64("id", segmentID))
		return errors.New("segment not found")
	}
	// Notify RootCoord segment is flushed
	req := &datapb.SegmentFlushCompletedMsg{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SegmentFlushDone,
		},
		Segment: segment.SegmentInfo,
	}
	resp, err := s.rootCoordClient.SegmentFlushCompleted(ctx, req)
	if err = VerifyResponse(resp, err); err != nil {
		log.Warn("failed to call SegmentFlushComplete", zap.Int64("segmentID", segmentID), zap.Error(err))
		return err
	}
	// set segment to SegmentState_Flushed
	if err = s.meta.SetState(segmentID, commonpb.SegmentState_Flushed); err != nil {
		log.Error("flush segment complete failed", zap.Error(err))
		return err
	}
	log.Debug("flush segment complete", zap.Int64("id", segmentID))
	return nil
}

// recovery logic, fetch all Segment in `Flushing` state and do Flush notification logic
func (s *Server) handleFlushingSegments(ctx context.Context) {
	segments := s.meta.GetFlushingSegments()
	for _, segment := range segments {
		select {
		case <-ctx.Done():
			return
		case s.flushCh <- segment.ID:
		}
	}
}

func (s *Server) initRootCoordClient() error {
	var err error
	if s.rootCoordClient, err = s.rootCoordClientCreator(s.ctx, Params.MetaRootPath, Params.EtcdEndpoints); err != nil {
		return err
	}
	if err = s.rootCoordClient.Init(); err != nil {
		return err
	}
	return s.rootCoordClient.Start()
}

// Stop do the Server finalize processes
// it checks the server status is healthy, if not, just quit
// if Server is healthy, set server state to stopped, release etcd session,
//	stop message stream client and stop server loops
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt64(&s.isServing, ServerStateHealthy, ServerStateStopped) {
		return nil
	}
	log.Debug("dataCoord server shutdown")
	s.cluster.Close()
	s.stopServerLoop()
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	log.Debug("clean meta", zap.Any("kv", s.kvClient))
	return s.kvClient.RemoveWithPrefix("")
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
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

func (s *Server) loadCollectionFromRootCoord(ctx context.Context, collectionID int64) error {
	resp, err := s.rootCoordClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
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
	presp, err := s.rootCoordClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.NodeID,
		},
		DbName:         "",
		CollectionName: resp.Schema.Name,
		CollectionID:   resp.CollectionID,
	})
	if err = VerifyResponse(presp, err); err != nil {
		log.Error("show partitions error", zap.String("collectionName", resp.Schema.Name),
			zap.Int64("collectionID", resp.CollectionID), zap.Error(err))
		return err
	}
	collInfo := &datapb.CollectionInfo{
		ID:             resp.CollectionID,
		Schema:         resp.Schema,
		Partitions:     presp.PartitionIDs,
		StartPositions: resp.GetStartPositions(),
	}
	s.meta.AddCollection(collInfo)
	return nil
}

// GetVChanPositions get vchannel latest postitions with provided dml channel names
func (s *Server) GetVChanPositions(vchans []vchannel, seekFromStartPosition bool) ([]*datapb.VchannelInfo, error) {
	if s.kvClient == nil {
		return nil, errNilKvClient
	}
	pairs := make([]*datapb.VchannelInfo, 0, len(vchans))

	for _, vchan := range vchans {
		segments := s.meta.GetSegmentsByChannel(vchan.DmlChannel)
		flushedSegmentIDs := make([]UniqueID, 0)
		unflushed := make([]*datapb.SegmentInfo, 0)
		var seekPosition *internalpb.MsgPosition
		var useUnflushedPosition bool
		for _, s := range segments {
			if s.State == commonpb.SegmentState_Flushing || s.State == commonpb.SegmentState_Flushed {
				flushedSegmentIDs = append(flushedSegmentIDs, s.ID)
				if seekPosition == nil || (!useUnflushedPosition && s.DmlPosition.Timestamp > seekPosition.Timestamp) {
					seekPosition = s.DmlPosition
				}
				continue
			}

			if s.DmlPosition == nil {
				continue
			}

			unflushed = append(unflushed, s.SegmentInfo)

			if seekPosition == nil || !useUnflushedPosition || s.DmlPosition.Timestamp < seekPosition.Timestamp {
				useUnflushedPosition = true
				if !seekFromStartPosition {
					seekPosition = s.DmlPosition
				} else {
					seekPosition = s.StartPosition
				}
			}
		}

		pairs = append(pairs, &datapb.VchannelInfo{
			CollectionID:      vchan.CollectionID,
			ChannelName:       vchan.DmlChannel,
			SeekPosition:      seekPosition,
			UnflushedSegments: unflushed,
			FlushedSegments:   flushedSegmentIDs,
		})
	}
	return pairs, nil
}
