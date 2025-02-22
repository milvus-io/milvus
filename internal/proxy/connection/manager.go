package connection

import (
	"container/heap"
	"context"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type connectionManager struct {
	initOnce sync.Once
	stopOnce sync.Once

	closeSignal chan struct{}
	wg          sync.WaitGroup

	clientInfos *typeutil.ConcurrentMap[int64, clientInfo]
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

	t := time.NewTimer(paramtable.Get().ProxyCfg.ConnectionCheckIntervalSeconds.GetAsDuration(time.Second))
	defer t.Stop()

	for {
		select {
		case <-s.closeSignal:
			log.Info("connection manager closed")
			return
		case <-t.C:
			s.removeLongInactiveClients()
			// not sure if we should purge them periodically.
			s.purgeIfNumOfClientsExceed()
			t.Reset(paramtable.Get().ProxyCfg.ConnectionCheckIntervalSeconds.GetAsDuration(time.Second))
		}
	}
}

func (s *connectionManager) purgeIfNumOfClientsExceed() {
	diffNum := int64(s.clientInfos.Len()) - paramtable.Get().ProxyCfg.MaxConnectionNum.GetAsInt64()
	if diffNum <= 0 {
		return
	}

	begin := time.Now()

	log := log.With(
		zap.Int64("num", int64(s.clientInfos.Len())),
		zap.Int64("limit", paramtable.Get().ProxyCfg.MaxConnectionNum.GetAsInt64()))

	log.Info("number of client infos exceed limit, ready to purge the oldest")
	q := newPriorityQueueWithCap(int(diffNum + 1))
	s.clientInfos.Range(func(identifier int64, info clientInfo) bool {
		heap.Push(&q, newQueryItem(info.identifier, info.lastActiveTime))
		if int64(q.Len()) > diffNum {
			// pop the newest.
			heap.Pop(&q)
		}
		return true
	})

	// time order doesn't matter here.
	for _, item := range q {
		info, exist := s.clientInfos.GetAndRemove(item.identifier)
		if exist {
			log.Info("remove client info", info.GetLogger()...)
		}
	}

	log.Info("purge client infos done",
		zap.Duration("cost", time.Since(begin)),
		zap.Int64("num after purge", int64(s.clientInfos.Len())))
}

func (s *connectionManager) Register(ctx context.Context, identifier int64, info *commonpb.ClientInfo) {
	cli := clientInfo{
		ClientInfo:     info,
		identifier:     identifier,
		lastActiveTime: time.Now(),
	}

	s.clientInfos.Insert(identifier, cli)
	log.Ctx(ctx).Info("client register", cli.GetLogger()...)
}

func (s *connectionManager) KeepActive(identifier int64) {
	s.Update(identifier)
}

func (s *connectionManager) List() []*commonpb.ClientInfo {
	clients := make([]*commonpb.ClientInfo, 0, s.clientInfos.Len())

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
	ttl := paramtable.Get().ProxyCfg.ConnectionClientInfoTTLSeconds.GetAsDuration(time.Second)
	s.clientInfos.Range(func(candidate int64, info clientInfo) bool {
		if time.Since(info.lastActiveTime) > ttl {
			log.Info("client deregister", info.GetLogger()...)
			s.clientInfos.Remove(candidate)
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
