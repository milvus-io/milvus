package distributed

import (
	"context"
	"errors"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"go.uber.org/zap"
)

type ConnectionManager struct {
	session *sessionutil.Session

	dependencies map[string]struct{}

	rootCoord    rootcoordpb.RootCoordClient
	rootCoordMu  sync.RWMutex
	queryCoord   querypb.QueryCoordClient
	queryCoordMu sync.RWMutex
	dataCoord    datapb.DataCoordClient
	dataCoordMu  sync.RWMutex
	indexCoord   indexpb.IndexCoordClient
	indexCoordMu sync.RWMutex
	queryNodes   map[int64]querypb.QueryNodeClient
	queryNodesMu sync.RWMutex
	dataNodes    map[int64]datapb.DataNodeClient
	dataNodesMu  sync.RWMutex
	indexNodes   map[int64]indexpb.IndexNodeClient
	indexNodesMu sync.RWMutex

	taskMu     sync.RWMutex
	buildTasks map[int64]*buildClientTask
	notify     chan int64

	connMu      sync.RWMutex
	connections map[int64]*grpc.ClientConn

	closeCh chan struct{}
}

func NewConnectionManager(session *sessionutil.Session) *ConnectionManager {
	return &ConnectionManager{
		session: session,

		dependencies: make(map[string]struct{}),

		queryNodes: make(map[int64]querypb.QueryNodeClient),
		dataNodes:  make(map[int64]datapb.DataNodeClient),
		indexNodes: make(map[int64]indexpb.IndexNodeClient),

		buildTasks: make(map[int64]*buildClientTask),
		notify:     make(chan int64),

		connections: make(map[int64]*grpc.ClientConn),
	}
}

func (cm *ConnectionManager) AddDependency(roleName string) error {
	if !cm.checkroleName(roleName) {
		return errors.New("roleName is illegal")
	}

	_, ok := cm.dependencies[roleName]
	if ok {
		log.Warn("Dependency is already added", zap.Any("roleName", roleName))
		return nil
	}
	cm.dependencies[roleName] = struct{}{}

	msess, rev, err := cm.session.GetSessions(roleName)
	if err != nil {
		log.Debug("ClientManager GetSessions failed", zap.Any("roleName", roleName))
		return err
	}

	if len(msess) == 0 {
		log.Debug("No nodes are currently alive", zap.Any("roleName", roleName))
	} else {
		for _, value := range msess {
			cm.buildConnections(value)
		}
	}

	eventChannel := cm.session.WatchServices(roleName, rev)
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

func (cm *ConnectionManager) GetIndexCoordClient() (indexpb.IndexCoordClient, bool) {
	cm.indexCoordMu.RLock()
	defer cm.indexCoordMu.RUnlock()
	_, ok := cm.dependencies[typeutil.IndexCoordRole]
	if !ok {
		log.Error("IndeCoord dependency has not been added yet")
		return nil, false
	}

	return cm.indexCoord, true
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

func (cm *ConnectionManager) GetIndexNodeClients() (map[int64]indexpb.IndexNodeClient, bool) {
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

//go:norace
// fix datarace in unittest
// startWatchService will only be invoked at start procedure
// otherwise, remove the annotation and add atomic protection
func (cm *ConnectionManager) processEvent(channel <-chan *sessionutil.SessionEvent) {
	for {
		select {
		case _, ok := <-cm.closeCh:
			if !ok {
				return
			}
		case ev, ok := <-channel:
			if !ok {
				//TODO silverxia add retry logic
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
			log.Debug("ConnectionManager", zap.Any("receive finish", serverID))
			if ok {
				log.Debug("ConnectionManager", zap.Any("get task ok", serverID))
				log.Debug("ConnectionManager", zap.Any("task state", task.state))
				if task.state == buildClientSuccess {
					log.Debug("ConnectionManager", zap.Any("build success", serverID))
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
	case typeutil.IndexCoordRole:
		cm.indexCoordMu.Lock()
		defer cm.indexCoordMu.Unlock()
		cm.indexCoord = indexpb.NewIndexCoordClient(connection)
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
		cm.indexNodes[session.ServerID] = indexpb.NewIndexNodeClient(connection)
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
			opts := trace.GetInterceptorOpts()
			log.Debug("Grpc connect ", zap.String("Address", bct.sess.Address))
			conn, err := grpc.DialContext(bct.ctx, bct.sess.Address,
				grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(30*time.Second),
				grpc.WithDisableRetry(),
				grpc.WithUnaryInterceptor(
					grpc_middleware.ChainUnaryClient(
						grpc_retry.UnaryClientInterceptor(
							grpc_retry.WithMax(3),
							grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
						),
						grpc_opentracing.UnaryClientInterceptor(opts...),
					)),
				grpc.WithStreamInterceptor(
					grpc_middleware.ChainStreamClient(
						grpc_retry.StreamClientInterceptor(
							grpc_retry.WithMax(3),
							grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
						),
						grpc_opentracing.StreamClientInterceptor(opts...),
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
		log.Debug("ConnectionManager", zap.Any("build connection finish", bct.sess.ServerID))
		if err != nil {
			log.Debug("BuildClientTask try connect failed",
				zap.Any("roleName", bct.sess.ServerName), zap.Error(err))
			bct.state = buildClientFailed
			return
		}
	}()
}
func (bct *buildClientTask) Stop() {
	bct.cancel()
}

func (bct *buildClientTask) finish() {
	log.Debug("ConnectionManager", zap.Any("notify connection finish", bct.sess.ServerID))
	bct.notify <- bct.sess.ServerID
}

var roles = map[string]struct{}{
	typeutil.RootCoordRole:  {},
	typeutil.QueryCoordRole: {},
	typeutil.DataCoordRole:  {},
	typeutil.IndexCoordRole: {},
	typeutil.QueryNodeRole:  {},
	typeutil.DataNodeRole:   {},
	typeutil.IndexNodeRole:  {},
}

func (cm *ConnectionManager) checkroleName(roleName string) bool {
	_, ok := roles[roleName]
	return ok
}
