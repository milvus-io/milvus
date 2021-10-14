package sessionutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// DefaultServiceRoot default root path used in kv by Session
	DefaultServiceRoot = "session/"
	// DefaultIDKey default id key for Session
	DefaultIDKey = "id"
	// DefaultRetryTimes default retry times when registerService or getServerByID
	DefaultRetryTimes = 30
	// DefaultTTL default ttl value when granting a lease
	DefaultTTL = 60
)

// SessionEventType session event type
type SessionEventType int

const (
	// SessionNoneEvent place holder for zero value
	SessionNoneEvent SessionEventType = iota
	// SessionAddEvent event type for a new Session Added
	SessionAddEvent
	// SessionDelEvent event type for a Session deleted
	SessionDelEvent
)

// Session is a struct to store service's session, including ServerID, ServerName,
// Address.
// Exclusive indicates that this server can only start one.
type Session struct {
	ctx        context.Context
	ServerID   int64  `json:"ServerID,omitempty"`
	ServerName string `json:"ServerName,omitempty"`
	Address    string `json:"Address,omitempty"`
	Exclusive  bool   `json:"Exclusive,omitempty"`

	liveCh   <-chan bool
	etcdCli  *clientv3.Client
	leaseID  clientv3.LeaseID
	cancel   context.CancelFunc
	metaRoot string
}

// NewSession is a helper to build Session object.
// ServerID, ServerName, Address, Exclusive will be assigned after Init().
// metaRoot is a path in etcd to save session information.
// etcdEndpoints is to init etcdCli when NewSession
func NewSession(ctx context.Context, metaRoot string, etcdEndpoints []string) *Session {
	ctx, cancel := context.WithCancel(ctx)
	session := &Session{
		ctx:      ctx,
		cancel:   cancel,
		metaRoot: metaRoot,
	}

	connectEtcdFn := func() error {
		log.Debug("Session try to connect to etcd")
		etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		ctx2, cancel2 := context.WithTimeout(session.ctx, 5*time.Second)
		defer cancel2()
		if _, err = etcdCli.Get(ctx2, "health"); err != nil {
			return err
		}
		session.etcdCli = etcdCli
		return nil
	}
	err := retry.Do(ctx, connectEtcdFn, retry.Attempts(300))
	if err != nil {
		log.Warn("failed to initialize session",
			zap.Error(err))
		return nil
	}
	log.Debug("Session connect to etcd success")
	return session
}

// Init will initialize base struct of the Session, including ServerName, ServerID,
// Address, Exclusive. ServerID is obtained in getServerID.
// Finally it will process keepAliveResponse to keep alive with etcd.
func (s *Session) Init(serverName, address string, exclusive bool) {
	s.ServerName = serverName
	s.Address = address
	s.Exclusive = exclusive
	s.checkIDExist()
	serverID, err := s.getServerID()
	if err != nil {
		panic(err)
	}
	s.ServerID = serverID
	ch, err := s.registerService()
	if err != nil {
		panic(err)
	}
	s.liveCh = s.processKeepAliveResponse(ch)
}

func (s *Session) getServerID() (int64, error) {
	return s.getServerIDWithKey(DefaultIDKey, DefaultRetryTimes)
}

func (s *Session) checkIDExist() {
	log.Debug("Session checkIDExist Begin")
	s.etcdCli.Txn(s.ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey)),
			"=",
			0)).
		Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey), "1")).Commit()
	log.Debug("Session checkIDExist End")
}

func (s *Session) getServerIDWithKey(key string, retryTimes uint) (int64, error) {
	for {
		log.Debug("Session try to get serverID")
		getResp, err := s.etcdCli.Get(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, key))
		if err != nil {
			log.Debug("Session get etcd key error", zap.String("key", key), zap.Error(err))
			return -1, err
		}
		if getResp.Count <= 0 {
			log.Debug("Session there is no value", zap.String("key", key))
			continue
		}
		value := string(getResp.Kvs[0].Value)
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Debug("Session ParseInt error", zap.String("value", value), zap.Error(err))
			continue
		}
		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Value(path.Join(s.metaRoot, DefaultServiceRoot, key)),
				"=",
				value)).
			Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, key), strconv.FormatInt(valueInt+1, 10))).Commit()
		if err != nil {
			log.Debug("Session Txn failed", zap.String("key", key), zap.Error(err))
			return -1, err
		}

		if !txnResp.Succeeded {
			log.Debug("Session Txn unsuccessful", zap.String("key", key))
			continue
		}
		log.Debug("Session get serverID success")
		return valueInt, nil
	}
}

// registerService registers the service to etcd so that other services
// can find that the service is online and issue subsequent operations
// RegisterService will save a key-value in etcd
// key: metaRootPath + "/services" + "/ServerName-ServerID"
// value: json format
// {
//   ServerID   int64  `json:"ServerID,omitempty"`
//	 ServerName string `json:"ServerName,omitempty"`
//	 Address    string `json:"Address,omitempty"`
//   Exclusive  bool   `json:"Exclusive,omitempty"`
// }
// Exclusive means whether this service can exist two at the same time, if so,
// it is false. Otherwise, set it to true.
func (s *Session) registerService() (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	var ch <-chan *clientv3.LeaseKeepAliveResponse
	log.Debug("Session Register Begin")
	registerFn := func() error {
		resp, err := s.etcdCli.Grant(s.ctx, DefaultTTL)
		if err != nil {
			log.Error("register service", zap.Error(err))
			return err
		}
		s.leaseID = resp.ID

		sessionJSON, err := json.Marshal(s)
		if err != nil {
			return err
		}

		key := s.ServerName
		if !s.Exclusive {
			key = key + "-" + strconv.FormatInt(s.ServerID, 10)
		}
		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Version(path.Join(s.metaRoot, DefaultServiceRoot, key)),
				"=",
				0)).
			Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, key), string(sessionJSON), clientv3.WithLease(resp.ID))).Commit()

		if err != nil {
			log.Warn("compare and swap error, maybe the key has ben registered", zap.Error(err))
			return err
		}

		if !txnResp.Succeeded {
			return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", key)
		}

		ch, err = s.etcdCli.KeepAlive(s.ctx, resp.ID)
		if err != nil {
			fmt.Printf("keep alive error %s\n", err)
			return err
		}
		log.Debug("Session Register End", zap.Int64("ServerID", s.ServerID))
		return nil
	}
	err := retry.Do(s.ctx, registerFn, retry.Attempts(DefaultRetryTimes), retry.Sleep(500*time.Millisecond))
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// processKeepAliveResponse processes the response of etcd keepAlive interface
// If keepAlive fails for unexpected error, it will send a signal to the channel.
func (s *Session) processKeepAliveResponse(ch <-chan *clientv3.LeaseKeepAliveResponse) (failChannel <-chan bool) {
	failCh := make(chan bool)
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				log.Error("keep alive", zap.Error(errors.New("context done")))
				return
			case resp, ok := <-ch:
				if !ok {
					log.Debug("session keepalive channel closed")
					close(failCh)
					return
				}
				if resp == nil {
					log.Debug("session keepalive response failed")
					close(failCh)
					return
				}
				//failCh <- true
			}
		}
	}()
	return failCh
}

// GetSessions will get all sessions registered in etcd.
// Revision is returned for WatchServices to prevent key events from being missed.
func (s *Session) GetSessions(prefix string) (map[string]*Session, int64, error) {
	res := make(map[string]*Session)
	key := path.Join(s.metaRoot, DefaultServiceRoot, prefix)
	resp, err := s.etcdCli.Get(s.ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, 0, err
	}
	log.Debug("SessionUtil GetSessions", zap.Any("prefix", prefix), zap.Any("resp", resp))
	for _, kv := range resp.Kvs {
		session := &Session{}
		err = json.Unmarshal(kv.Value, session)
		if err != nil {
			return nil, 0, err
		}
		_, mapKey := path.Split(string(kv.Key))
		res[mapKey] = session
	}
	return res, resp.Header.Revision, nil
}

// SessionEvent indicates the changes of other servers.
// if a server is up, EventType is SessAddEvent.
// if a server is down, EventType is SessDelEvent.
// Session Saves the changed server's information.
type SessionEvent struct {
	EventType SessionEventType
	Session   *Session
}

// WatchServices watch the service's up and down in etcd, and send event to
// eventChannel.
// prefix is a parameter to know which service to watch and can be obtained in
// typeutil.type.go.
// revision is a etcd reversion to prevent missing key events and can be obtained
// in GetSessions.
// If a server up, a event will be add to channel with eventType SessionAddType.
// If a server down, a event will be add to channel with eventType SessionDelType.
func (s *Session) WatchServices(prefix string, revision int64) (eventChannel <-chan *SessionEvent) {
	eventCh := make(chan *SessionEvent, 100)
	rch := s.etcdCli.Watch(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case wresp, ok := <-rch:
				if !ok {
					return
				}
				if wresp.Err() != nil {
					//close event channel
					log.Warn("Watch service found error", zap.Error(wresp.Err()))
					close(eventCh)
					return
				}
				for _, ev := range wresp.Events {
					session := &Session{}
					var eventType SessionEventType
					switch ev.Type {
					case mvccpb.PUT:
						log.Debug("watch services",
							zap.Any("add kv", ev.Kv))
						err := json.Unmarshal([]byte(ev.Kv.Value), session)
						if err != nil {
							log.Error("watch services", zap.Error(err))
							continue
						}
						eventType = SessionAddEvent
					case mvccpb.DELETE:
						log.Debug("watch services",
							zap.Any("delete kv", ev.PrevKv))
						err := json.Unmarshal([]byte(ev.PrevKv.Value), session)
						if err != nil {
							log.Error("watch services", zap.Error(err))
							continue
						}
						eventType = SessionDelEvent
					}
					log.Debug("WatchService", zap.Any("event type", eventType))
					eventCh <- &SessionEvent{
						EventType: eventType,
						Session:   session,
					}
				}

			}
		}
	}()
	return eventCh
}

// LivenessCheck performs liveness check with provided context and channel
// ctx controls the liveness check loop
// ch is the liveness signal channel, ch is closed only when the session is expired
// callback is the function to call when ch is closed, note that callback will not be invoked when loop exits due to context
func (s *Session) LivenessCheck(ctx context.Context, callback func()) {
	for {
		select {
		case _, ok := <-s.liveCh:
			// ok, still alive
			if ok {
				continue
			}
			// not ok, connection lost
			log.Warn("connection lost detected, shuting down")
			if callback != nil {
				go callback()
			}
			return
		case <-ctx.Done():
			log.Debug("liveness exits due to context done")
			return
		}
	}
}
