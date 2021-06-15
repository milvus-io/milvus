package sessionutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	DefaultServiceRoot = "session/"
	DefaultIDKey       = "id"
	DefaultRetryTimes  = 30
	DefaultTTL         = 10
)

type SessionEventType int

const (
	SessionNoneEvent SessionEventType = iota
	SessionAddEvent
	SessionDelEvent
)

// Session is a struct to store service's session, including ServerID, ServerName,
// Address.
// LeaseID will be assigned after registered in etcd.
type Session struct {
	ctx        context.Context
	ServerID   int64  `json:"ServerID,omitempty"`
	ServerName string `json:"ServerName,omitempty"`
	Address    string `json:"Address,omitempty"`
	Exclusive  bool   `json:"Exclusive,omitempty"`

	etcdCli  *clientv3.Client
	leaseID  clientv3.LeaseID
	cancel   context.CancelFunc
	metaRoot string
}

type SessionEvent struct {
	EventType SessionEventType
	Session   *Session
}

// NewSession is a helper to build Session object.
// ServerID and LeaseID will be assigned after registeration.
// etcdCli is initialized when NewSession
func NewSession(ctx context.Context, metaRoot string, etcdEndpoints []string) *Session {
	ctx, cancel := context.WithCancel(ctx)
	session := &Session{
		ctx:      ctx,
		cancel:   cancel,
		metaRoot: metaRoot,
	}

	connectEtcdFn := func() error {
		etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		session.etcdCli = etcdCli
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return nil
	}
	return session
}

// Init will initialize base struct in the SessionManager, including getServerID,
// and process keepAliveResponse
func (s *Session) Init(serverName, address string, exclusive bool) <-chan bool {
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
	return s.processKeepAliveResponse(ch)
}

// getServerID gets id from etcd with key: "/session"+"id"
// Each server get ServerID and add one to id.
func (s *Session) getServerID() (int64, error) {
	return s.getServerIDWithKey(DefaultIDKey, DefaultRetryTimes)
}

func (s *Session) checkIDExist() {
	s.etcdCli.Txn(s.ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey)),
			"=",
			0)).
		Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey), "1")).Commit()

}

func (s *Session) getServerIDWithKey(key string, retryTimes int) (int64, error) {
	res := int64(0)
	getServerIDWithKeyFn := func() error {
		getResp, err := s.etcdCli.Get(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, key))
		if err != nil {
			return nil
		}
		if getResp.Count <= 0 {
			return fmt.Errorf("there is no value on key = %s", key)
		}
		value := string(getResp.Kvs[0].Value)
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Debug("session", zap.Error(err))
			return err
		}
		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Value(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey)),
				"=",
				value)).
			Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey), strconv.FormatInt(valueInt+1, 10))).Commit()
		if err != nil {
			return err
		}

		if !txnResp.Succeeded {
			return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", key)
		}
		res = valueInt
		return nil
	}

	err := retry.Retry(retryTimes, time.Millisecond*500, getServerIDWithKeyFn)
	return res, err
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
			fmt.Printf("compare and swap error %s\n. maybe the key has registered", err)
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
		return nil
	}
	err := retry.Retry(DefaultRetryTimes, time.Millisecond*500, registerFn)
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
					close(failCh)
					return
				}
				if resp == nil {
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

// WatchServices watch the service's up and down in etcd, and saves it into local
// sessions.
// If a server up, it will be add to addChannel.
// If a server is offline, it will be add to delChannel.
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

func initEtcd(etcdEndpoints []string) (*clientv3.Client, error) {
	var etcdCli *clientv3.Client
	connectEtcdFn := func() error {
		etcd, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		etcdCli = etcd
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return nil, err
	}
	return etcdCli, nil
}
