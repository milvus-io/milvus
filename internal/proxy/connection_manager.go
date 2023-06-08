package proxy

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/milvus-io/milvus/pkg/log"
)

const (
	// we shouldn't check this too frequently.
	defaultConnCheckDuration  = 2 * time.Minute
	defaultTTLForInactiveConn = 24 * time.Hour
)

type connectionManager struct {
	mu sync.RWMutex

	initOnce sync.Once
	stopOnce sync.Once

	closeSignal chan struct{}
	wg          sync.WaitGroup

	buffer   chan int64
	duration time.Duration
	ttl      time.Duration

	clientInfos map[int64]clientInfo
}

type connectionManagerOption func(s *connectionManager)

func withDuration(duration time.Duration) connectionManagerOption {
	return func(s *connectionManager) {
		s.duration = duration
	}
}

func withTTL(ttl time.Duration) connectionManagerOption {
	return func(s *connectionManager) {
		s.ttl = ttl
	}
}

func (s *connectionManager) apply(opts ...connectionManagerOption) {
	for _, opt := range opts {
		opt(s)
	}
}

func (s *connectionManager) init() {
	s.initOnce.Do(func() {
		s.wg.Add(1)
		go s.checkLoop()
	})
}

func (s *connectionManager) stop() {
	s.stopOnce.Do(func() {
		close(s.closeSignal)
		s.wg.Wait()
	})
}

func (s *connectionManager) checkLoop() {
	defer s.wg.Done()

	t := time.NewTicker(s.duration)
	defer t.Stop()

	for {
		select {
		case <-s.closeSignal:
			log.Info("connection manager closed")
			return
		case identifier := <-s.buffer:
			s.update(identifier)
		case <-t.C:
			s.removeLongInactiveClients()
		}
	}
}

func (s *connectionManager) register(ctx context.Context, identifier int64, info *commonpb.ClientInfo) {
	cli := clientInfo{
		ClientInfo:     info,
		identifier:     identifier,
		lastActiveTime: time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.clientInfos[identifier] = cli
	cli.ctxLogRegister(ctx)
}

func (s *connectionManager) keepActive(identifier int64) {
	// make this asynchronous and then the rpc won't be blocked too long.
	s.buffer <- identifier
}

func (s *connectionManager) update(identifier int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cli, ok := s.clientInfos[identifier]
	if ok {
		cli.lastActiveTime = time.Now()
		s.clientInfos[identifier] = cli
	}
}

func (s *connectionManager) removeLongInactiveClients() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for candidate, cli := range s.clientInfos {
		if time.Since(cli.lastActiveTime) > s.ttl {
			cli.logDeregister()
			delete(s.clientInfos, candidate)
		}
	}
}

func (s *connectionManager) list() []*commonpb.ClientInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clients := make([]*commonpb.ClientInfo, 0, len(s.clientInfos))

	for identifier, cli := range s.clientInfos {
		if cli.ClientInfo != nil {
			client := proto.Clone(cli.ClientInfo).(*commonpb.ClientInfo)
			if client.Reserved == nil {
				client.Reserved = make(map[string]string)
			}
			client.Reserved["identifier"] = string(strconv.AppendInt(nil, identifier, 10))
			client.Reserved["last_active_time"] = cli.lastActiveTime.String()

			clients = append(clients, client)
		}
	}

	return clients
}

func newConnectionManager(opts ...connectionManagerOption) *connectionManager {
	s := &connectionManager{
		closeSignal: make(chan struct{}, 1),
		buffer:      make(chan int64, 64),
		duration:    defaultConnCheckDuration,
		ttl:         defaultTTLForInactiveConn,
		clientInfos: make(map[int64]clientInfo),
	}
	s.apply(opts...)
	s.init()

	return s
}

var connectionManagerInstance *connectionManager

var getConnectionManagerInstanceOnce sync.Once

func GetConnectionManager() *connectionManager {
	getConnectionManagerInstanceOnce.Do(func() {
		connectionManagerInstance = newConnectionManager(
			withDuration(defaultConnCheckDuration),
			withTTL(defaultTTLForInactiveConn))
	})
	return connectionManagerInstance
}
