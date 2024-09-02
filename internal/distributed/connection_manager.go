// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distributed

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ConnectionManager handles connection to other components of the system
type ConnectionManager struct {
	session *sessionutil.Session

	dependencies map[string]struct{}

	rootCoord    rootcoordpb.RootCoordClient
	rootCoordMu  sync.RWMutex
	queryCoord   querypb.QueryCoordClient
	queryCoordMu sync.RWMutex
	dataCoord    datapb.DataCoordClient
	dataCoordMu  sync.RWMutex
	queryNodes   map[int64]querypb.QueryNodeClient
	queryNodesMu sync.RWMutex
	dataNodes    map[int64]datapb.DataNodeClient
	dataNodesMu  sync.RWMutex
	indexNodes   map[int64]workerpb.IndexNodeClient
	indexNodesMu sync.RWMutex

	taskMu     sync.RWMutex
	buildTasks map[int64]*buildClientTask
	notify     chan int64

	connMu      sync.RWMutex
	connections map[int64]*grpc.ClientConn

	closeCh chan struct{}
}

// NewConnectionManager creates a new connection manager.
func NewConnectionManager(session *sessionutil.Session) *ConnectionManager {
	return &ConnectionManager{
		session: session,

		dependencies: make(map[string]struct{}),

		queryNodes: make(map[int64]querypb.QueryNodeClient),
		dataNodes:  make(map[int64]datapb.DataNodeClient),
		indexNodes: make(map[int64]workerpb.IndexNodeClient),

		buildTasks: make(map[int64]*buildClientTask),
		notify:     make(chan int64),

		connections: make(map[int64]*grpc.ClientConn),
	}
}

// AddDependency add a dependency by role name.
func (cm *ConnectionManager) AddDependency(roleName string) error {
	if !cm.checkroleName(roleName) {
		return errors.New("roleName is illegal")
	}

	_, ok := cm.dependencies[roleName]
	if ok {
		log.Warn("Dependency is already added", zap.String("roleName", roleName))
		return nil
	}
	cm.dependencies[roleName] = struct{}{}

	msess, rev, err := cm.session.GetSessions(roleName)
	if err != nil {
		log.Debug("ClientManager GetSessions failed", zap.String("roleName", roleName))
		return err
	}

	if len(msess) == 0 {
		log.Debug("No nodes are currently alive", zap.String("roleName", roleName))
	} else {
		for _, value := range msess {
			cm.buildConnections(value)
		}
	}

	eventChannel := cm.session.WatchServices(roleName, rev, nil)
	go cm.processEvent(eventChannel)

	return nil
}

func (cm *ConnectionManager) Start() {
	go cm.receiveFinishTask()
}

func (cm *ConnectionManager) GetRootCoordClient() (rootcoordpb.RootCoordClient, bool) {
	cm.rootCoordMu.RLock()
	defer cm.rootCoordMu.RUnlock()
	_, ok := cm.dependencies[typeutil.RootCoordRole]
	if !ok {
		log.Error("RootCoord dependency has not been added yet")
		return nil, false
	}

	return cm.rootCoord, true
}

func (cm *ConnectionManager) GetQueryCoordClient() (querypb.QueryCoordClient, bool) {
	cm.queryCoordMu.RLock()
	defer cm.queryCoordMu.RUnlock()
	_, ok := cm.dependencies[typeutil.QueryCoordRole]
	if !ok {
		log.Error("QueryCoord dependency has not been added yet")
		return nil, false
	}

	return cm.queryCoord, true
}

func (cm *ConnectionManager) GetDataCoordClient() (datapb.DataCoordClient, bool) {
	cm.dataCoordMu.RLock()
	defer cm.dataCoordMu.RUnlock()
	_, ok := cm.dependencies[typeutil.DataCoordRole]
	if !ok {
		log.Error("DataCoord dependency has not been added yet")
		return nil, false
	}

	return cm.dataCoord, true
}

func (cm *ConnectionManager) GetQueryNodeClients() (map[int64]querypb.QueryNodeClient, bool) {
	cm.queryNodesMu.RLock()
	defer cm.queryNodesMu.RUnlock()
	_, ok := cm.dependencies[typeutil.QueryNodeRole]
	if !ok {
		log.Error("QueryNode dependency has not been added yet")
		return nil, false
	}

	return cm.queryNodes, true
}

func (cm *ConnectionManager) GetDataNodeClients() (map[int64]datapb.DataNodeClient, bool) {
	cm.dataNodesMu.RLock()
	defer cm.dataNodesMu.RUnlock()
	_, ok := cm.dependencies[typeutil.DataNodeRole]
	if !ok {
		log.Error("DataNode dependency has not been added yet")
		return nil, false
	}

	return cm.dataNodes, true
}

func (cm *ConnectionManager) GetIndexNodeClients() (map[int64]workerpb.IndexNodeClient, bool) {
	cm.indexNodesMu.RLock()
	defer cm.indexNodesMu.RUnlock()
	_, ok := cm.dependencies[typeutil.IndexNodeRole]
	if !ok {
		log.Error("IndexNode dependency has not been added yet")
		return nil, false
	}

	return cm.indexNodes, true
}

func (cm *ConnectionManager) Stop() {
	for _, task := range cm.buildTasks {
		task.Stop()
	}
	close(cm.closeCh)
	for _, conn := range cm.connections {
		conn.Close()
	}
}

// fix datarace in unittest
// startWatchService will only be invoked at start procedure
// otherwise, remove the annotation and add atomic protection
//
//go:norace
func (cm *ConnectionManager) processEvent(channel <-chan *sessionutil.SessionEvent) {
	for {
		select {
		case _, ok := <-cm.closeCh:
			if !ok {
				return
			}
		case ev, ok := <-channel:
			if !ok {
				log.Error("watch service channel closed", zap.Int64("serverID", cm.session.ServerID))
				go cm.Stop()
				if cm.session.TriggerKill {
					if p, err := os.FindProcess(os.Getpid()); err == nil {
						p.Signal(syscall.SIGINT)
					}
				}
				return
			}
			switch ev.EventType {
			case sessionutil.SessionAddEvent:
				log.Debug("ConnectionManager", zap.Any("add event", ev.Session))
				cm.buildConnections(ev.Session)
			case sessionutil.SessionDelEvent:
				cm.removeTask(ev.Session.ServerID)
				cm.removeConnection(ev.Session.ServerID)
			}
		}
	}
}

func (cm *ConnectionManager) receiveFinishTask() {
	for {
		select {
		case _, ok := <-cm.closeCh:
			if !ok {
				return
			}
		case serverID := <-cm.notify:
			cm.taskMu.Lock()
			task, ok := cm.buildTasks[serverID]
			log.Debug("ConnectionManager", zap.Int64("receive finish", serverID))
			if ok {
				log.Debug("ConnectionManager", zap.Int64("get task ok", serverID))
				log.Debug("ConnectionManager", zap.Any("task state", task.state))
				if task.state == buildClientSuccess {
					log.Debug("ConnectionManager", zap.Int64("build success", serverID))
					cm.addConnection(task.sess.ServerID, task.result)
					cm.buildClients(task.sess, task.result)
				}
				delete(cm.buildTasks, serverID)
			}
			cm.taskMu.Unlock()
		}
	}
}

func (cm *ConnectionManager) buildClients(session *sessionutil.Session, connection *grpc.ClientConn) {
	switch session.ServerName {
	case typeutil.RootCoordRole:
		cm.rootCoordMu.Lock()
		defer cm.rootCoordMu.Unlock()
		cm.rootCoord = rootcoordpb.NewRootCoordClient(connection)
	case typeutil.DataCoordRole:
		cm.dataCoordMu.Lock()
		defer cm.dataCoordMu.Unlock()
		cm.dataCoord = datapb.NewDataCoordClient(connection)
	case typeutil.QueryCoordRole:
		cm.queryCoordMu.Lock()
		defer cm.queryCoordMu.Unlock()
		cm.queryCoord = querypb.NewQueryCoordClient(connection)
	case typeutil.QueryNodeRole:
		cm.queryNodesMu.Lock()
		defer cm.queryNodesMu.Unlock()
		cm.queryNodes[session.ServerID] = querypb.NewQueryNodeClient(connection)
	case typeutil.DataNodeRole:
		cm.dataNodesMu.Lock()
		defer cm.dataNodesMu.Unlock()
		cm.dataNodes[session.ServerID] = datapb.NewDataNodeClient(connection)
	case typeutil.IndexNodeRole:
		cm.indexNodesMu.Lock()
		defer cm.indexNodesMu.Unlock()
		cm.indexNodes[session.ServerID] = workerpb.NewIndexNodeClient(connection)
	}
}

func (cm *ConnectionManager) buildConnections(session *sessionutil.Session) {
	task := newBuildClientTask(session, cm.notify)
	cm.addTask(session.ServerID, task)
	task.Run()
}

func (cm *ConnectionManager) addConnection(id int64, conn *grpc.ClientConn) {
	cm.connMu.Lock()
	cm.connections[id] = conn
	cm.connMu.Unlock()
}

func (cm *ConnectionManager) removeConnection(id int64) {
	cm.connMu.Lock()
	conn, ok := cm.connections[id]
	if ok {
		conn.Close()
		delete(cm.connections, id)
	}
	cm.connMu.Unlock()
}

func (cm *ConnectionManager) addTask(id int64, task *buildClientTask) {
	cm.taskMu.Lock()
	cm.buildTasks[id] = task
	cm.taskMu.Unlock()
}

func (cm *ConnectionManager) removeTask(id int64) {
	cm.taskMu.Lock()
	task, ok := cm.buildTasks[id]
	if ok {
		task.Stop()
		delete(cm.buildTasks, id)
	}
	cm.taskMu.Unlock()
}

type buildConnectionstate int

const (
	buildConnectionstart buildConnectionstate = iota
	buildClientRunning
	buildClientSuccess
	buildClientFailed
)

type buildClientTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	sess         *sessionutil.Session
	state        buildConnectionstate
	retryOptions []retry.Option

	result *grpc.ClientConn
	notify chan int64
}

func newBuildClientTask(session *sessionutil.Session, notify chan int64, retryOptions ...retry.Option) *buildClientTask {
	ctx, cancel := context.WithCancel(context.Background())
	return &buildClientTask{
		ctx:    ctx,
		cancel: cancel,

		sess:         session,
		retryOptions: retryOptions,

		notify: notify,
	}
}

func (bct *buildClientTask) Run() {
	bct.state = buildClientRunning
	go func() {
		defer bct.finish()
		connectGrpcFunc := func() error {
			opts := tracer.GetInterceptorOpts()
			log.Debug("Grpc connect", zap.String("Address", bct.sess.Address))
			ctx, cancel := context.WithTimeout(bct.ctx, 30*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(ctx, bct.sess.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
				grpc.WithDisableRetry(),
				grpc.WithUnaryInterceptor(
					grpc_middleware.ChainUnaryClient(
						grpc_retry.UnaryClientInterceptor(
							grpc_retry.WithMax(3),
							grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
						),
						otelgrpc.UnaryClientInterceptor(opts...),
					)),
				grpc.WithStreamInterceptor(
					grpc_middleware.ChainStreamClient(
						grpc_retry.StreamClientInterceptor(
							grpc_retry.WithMax(3),
							grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
						),
						otelgrpc.StreamClientInterceptor(opts...),
					)),
			)
			if err != nil {
				return err
			}
			bct.result = conn
			bct.state = buildClientSuccess
			return nil
		}

		err := retry.Do(bct.ctx, connectGrpcFunc, bct.retryOptions...)
		log.Debug("ConnectionManager", zap.Int64("build connection finish", bct.sess.ServerID))
		if err != nil {
			log.Debug("BuildClientTask try connect failed",
				zap.String("roleName", bct.sess.ServerName), zap.Error(err))
			bct.state = buildClientFailed
			return
		}
	}()
}

func (bct *buildClientTask) Stop() {
	bct.cancel()
}

func (bct *buildClientTask) finish() {
	log.Debug("ConnectionManager", zap.Int64("notify connection finish", bct.sess.ServerID))
	bct.notify <- bct.sess.ServerID
}

var roles = map[string]struct{}{
	typeutil.RootCoordRole:  {},
	typeutil.QueryCoordRole: {},
	typeutil.DataCoordRole:  {},
	typeutil.QueryNodeRole:  {},
	typeutil.DataNodeRole:   {},
	typeutil.IndexNodeRole:  {},
}

func (cm *ConnectionManager) checkroleName(roleName string) bool {
	_, ok := roles[roleName]
	return ok
}
