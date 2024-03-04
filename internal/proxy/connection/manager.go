package connection

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/atomic"
)

type connectionManager struct {
	initOnce sync.Once
	stopOnce sync.Once

	closeSignal chan struct{}
	wg          sync.WaitGroup

	clientInfos *typeutil.ConcurrentMap[int64, clientInfo]
	count       atomic.Int64
}

func (s *connectionManager) init() {
	s.initOnce.Do(func() {
		s.wg.Add(1)
		go s.checkLoop()
	})
}

func (s *connectionManager) Stop() {
	s.stopOnce.Do(func() {
		close(s.closeSignal)
		s.wg.Wait()
	})
}

func (s *connectionManager) checkLoop() {
	defer s.wg.Done()

	t := time.NewTimer(paramtable.Get().ProxyCfg.ConnectionCheckInterval.GetAsDuration(time.Second))

	for {
		select {
		case <-s.closeSignal:
			if !t.Stop() {
				<-t.C
			}
			log.Info("connection manager closed")
			return
		case <-t.C:
			s.removeLongInactiveClients()
			t = time.NewTimer(paramtable.Get().ProxyCfg.ConnectionCheckInterval.GetAsDuration(time.Second))
		}
	}
}

func (s *connectionManager) Register(ctx context.Context, identifier int64, info *commonpb.ClientInfo) {
	cli := clientInfo{
		ClientInfo:     info,
		identifier:     identifier,
		lastActiveTime: time.Now(),
	}

	s.count.Inc()
	s.clientInfos.Insert(identifier, cli)
	log.Ctx(ctx).Info("client register", cli.GetLogger()...)
}

func (s *connectionManager) KeepActive(identifier int64) {
	s.Update(identifier)
}

func (s *connectionManager) List() []*commonpb.ClientInfo {
	clients := make([]*commonpb.ClientInfo, 0, s.count.Load())

	s.clientInfos.Range(func(identifier int64, info clientInfo) bool {
		if info.ClientInfo != nil {
			client := typeutil.Clone(info.ClientInfo)
			if client.Reserved == nil {
				client.Reserved = make(map[string]string)
			}
			client.Reserved["identifier"] = string(strconv.AppendInt(nil, identifier, 10))
			client.Reserved["last_active_time"] = info.lastActiveTime.String()

			clients = append(clients, client)
		}
		return true
	})

	return clients
}

func (s *connectionManager) Get(ctx context.Context) *commonpb.ClientInfo {
	identifier, err := GetIdentifierFromContext(ctx)
	if err != nil {
		return nil
	}

	cli, ok := s.clientInfos.Get(identifier)
	if !ok {
		return nil
	}
	return cli.ClientInfo
}

func (s *connectionManager) Update(identifier int64) {
	info, ok := s.clientInfos.Get(identifier)
	if ok {
		info.lastActiveTime = time.Now()
		s.clientInfos.Insert(identifier, info)
	}
}

func (s *connectionManager) removeLongInactiveClients() {
	ttl := paramtable.Get().ProxyCfg.ConnectionClientInfoTTL.GetAsDuration(time.Second)
	s.clientInfos.Range(func(candidate int64, info clientInfo) bool {
		if time.Since(info.lastActiveTime) > ttl {
			log.Info("client deregister", info.GetLogger()...)
			s.clientInfos.Remove(candidate)
			s.count.Dec()
		}
		return true
	})
}

func newConnectionManager() *connectionManager {
	s := &connectionManager{
		closeSignal: make(chan struct{}, 1),
		clientInfos: typeutil.NewConcurrentMap[int64, clientInfo](),
	}
	s.init()

	return s
}
