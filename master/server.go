// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"

	"github.com/czs007/suvlim/master/kv"

	//"fmt"
	"math/rand"

	"github.com/czs007/suvlim/master/member"

	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/czs007/suvlim/errors"

	//"github.com/czs007/suvlim/pkg/"
	"github.com/czs007/suvlim/pkg/pdpb"
	"github.com/czs007/suvlim/util/etcdutil"
	"github.com/pingcap/log"

	"github.com/czs007/suvlim/master/config"
	"github.com/czs007/suvlim/master/id"
	"github.com/czs007/suvlim/master/meta"
	"github.com/czs007/suvlim/util/logutil"
	"github.com/czs007/suvlim/util/typeutil"

	//"github.com/czs007/suvlim/master/kv"
	"github.com/czs007/suvlim/master/tso"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"

	//"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	etcdTimeout     = time.Second * 3
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdClusterIDPath = "/pd/cluster_id"
)

var (
	// EnableZap enable the zap logger in embed etcd.
	EnableZap = true
	// EtcdStartTimeout the timeout of the startup etcd.
	EtcdStartTimeout = time.Minute * 5
)

// Server is the pd server.
type Server struct {
	// Server state.
	isServing int64

	// Server start timestamp
	startTimestamp int64

	// Configs and initial fields.
	cfg            *config.Config
	etcdCfg        *embed.Config
	persistOptions *config.PersistOptions

	// for PD leader election.
	member *member.Member

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.

	// handler        *Handler

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	// etcd client
	client    *clientv3.Client
	clusterID uint64 // pd cluster id.
	rootPath  string

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAllocator *id.AllocatorImpl
	storage     *meta.Storage

	// for tso.
	tsoAllocator tso.Allocator
	//

	hbStreams *heartbeatStreams

	// Zap logger
	lg       *zap.Logger
	logProps *log.ZapProperties

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *config.Config) (*Server, error) {
	log.Info("PD Config", zap.Reflect("config", cfg))
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		cfg:            cfg,
		persistOptions: config.NewPersistOptions(cfg),
		member:         &member.Member{},
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}

	// s.handler = newHandler(s)

	// Adjust etcd config.
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) {
		pdpb.RegisterPDServer(gs, s)
		//diagnosticspb.RegisterDiagnosticsServer(gs, s)
	}
	s.etcdCfg = etcdCfg

	s.lg = cfg.GetZapLogger()
	s.logProps = cfg.GetZapLogProperties()
	s.lg = cfg.GetZapLogger()
	s.logProps = cfg.GetZapLogProperties()
	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	newCtx, cancel := context.WithTimeout(ctx, EtcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errors.WithStack(err)
	}

	// Check cluster ID
	//urlMap, err := types.NewURLsMap(s.cfg.InitialCluster)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}

	//if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlMap, tlsConfig); err != nil {
	//	return err
	//}
	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-newCtx.Done():
		return errors.Errorf("canceled when waiting embed etcd to be ready")
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints), zap.Reflect("cert", s.cfg.Security))

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return err
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Info("update advertise peer urls", zap.String("from", s.cfg.AdvertisePeerUrls), zap.String("to", etcdPeerURLs))
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}
	s.client = client

	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

func (s *Server) startServer(ctx context.Context) error {
	var err error
	if err = s.initClusterID(); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// It may lose accuracy if use float64 to store uint64. So we store the
	// cluster id in label.
	//metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)
	kvBase := kv.NewEtcdKVBase(s.client, s.rootPath)
	// path := filepath.Join(s.cfg.DataDir, "region-meta")

	// regionStorage, err := core.NewRegionStorage(ctx, path)
	// if err != nil {
	// return err
	// }
	s.storage = meta.NewStorage(kvBase)
	// s.basicCluster = core.NewBasicCluster()
	// s.cluster = cluster.NewRaftCluster(ctx, s.GetClusterRootPath(), s.clusterID, syncer.NewRegionSyncer(s), s.client, s.httpClient)
	s.hbStreams = newHeartbeatStreams(ctx, s.clusterID)

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := etcdutil.EtcdKVGet(s.client, pdClusterIDPath)
	if err != nil {
		return err
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return err
	}
	s.clusterID, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
	return err
}

// AddCloseCallback adds a callback in the Close phase.
func (s *Server) AddCloseCallback(callbacks ...func()) {
	s.closeCallbacks = append(s.closeCallbacks, callbacks...)
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.stopServerLoop()

	if s.client != nil {
		s.client.Close()
	}

	if s.member.Etcd() != nil {
		s.member.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}
	if err := s.storage.Close(); err != nil {
		log.Error("close storage meet error", zap.Error(err))
	}

	// Run callbacks
	for _, cb := range s.closeCallbacks {
		cb()
	}

	log.Info("close server")
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// Run runs the pd server.
func (s *Server) Run() error {
	//go StartMonitor(s.ctx, time.Now, func() {
	//	log.Error("system time jumps backward")
	//	timeJumpBackCounter.Inc()
	//})
	if err := s.startEtcd(s.ctx); err != nil {
		return err
	}
	if err := s.startServer(s.ctx); err != nil {
		return err
	}

	s.startServerLoop(s.ctx)

	return nil
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// LoopContext returns the loop context of server.
func (s *Server) LoopContext() context.Context {
	return s.serverLoopCtx
}

func (s *Server) startServerLoop(ctx context.Context) {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(ctx)
	s.serverLoopWg.Add(1)
	go s.etcdLeaderLoop()
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetClientScheme returns the client URL scheme
func (s *Server) GetClientScheme() string {
	if len(s.cfg.Security.CertPath) == 0 && len(s.cfg.Security.KeyPath) == 0 {
		return "http"
	}
	return "https"
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetPersistOptions returns the schedule option.
func (s *Server) GetPersistOptions() *config.PersistOptions {
	return s.persistOptions
}

// GetStorage returns the backend storage of server.
func (s *Server) GetStorage() *meta.Storage {
	return s.storage
}

// GetHBStreams returns the heartbeat streams.
func (s *Server) GetHBStreams() HeartbeatStreams {
	return s.hbStreams
}

// GetAllocator returns the ID allocator of server.
func (s *Server) GetAllocator() *id.AllocatorImpl {
	return s.idAllocator
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// StartTimestamp returns the start timestamp of this server
func (s *Server) StartTimestamp() int64 {
	return s.startTimestamp
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *config.Config {
	cfg := s.cfg.Clone()
	cfg.PDServerCfg = *s.persistOptions.GetPDServerConfig()
	storage := s.GetStorage()
	if storage == nil {
		return cfg
	}
	return cfg
}

// GetServerOption gets the option of the server.
func (s *Server) GetServerOption() *config.PersistOptions {
	return s.persistOptions
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) error {
	if !isLevelLegal(level) {
		return errors.Errorf("log level %s is illegal", level)
	}
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
	return nil
}

func isLevelLegal(level string) bool {
	switch strings.ToLower(level) {
	case "fatal", "error", "warn", "warning", "debug", "info":
		return true
	default:
		return false
	}
}

func (s *Server) etcdLeaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		// case <-time.After(s.cfg.LeaderPriorityCheckInterval.Duration):
		// 	s.member.CheckPriority(ctx)
		case <-ctx.Done():
			log.Info("server is closed, exit etcd leader loop")
			return
		}
	}
}
