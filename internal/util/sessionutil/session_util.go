package sessionutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const defaultServiceRoot = "/services/"
const defaultIDKey = "id"
const defaultRetryTimes = 30
const defaultTTL = 10

// Session is a struct to store service's session, including ServerID, ServerName,
// Address.
// LeaseID will be assigned after registered in etcd.
type Session struct {
	ServerID   int64
	ServerName string
	Address    string
	Exclusive  bool
	LeaseID    clientv3.LeaseID
}

var (
	globalSessionManager = &SessionManager{}
)

// SessionManager is a struct to help store other service's session.
// including ServerID, ServerName, Address.
// It can fetch up-to-date sessions' information and watch service up and down.
type SessionManager struct {
	ctx    context.Context
	etcdKV *etcdkv.EtcdKV

	Self     *Session
	Sessions sync.Map
}

// NewSession is a helper to build Session object.LeaseID will be assigned after
// registeration.
func NewSession(serverName, address string, exclusive bool) *Session {
	return &Session{
		ServerName: serverName,
		Address:    address,
		Exclusive:  exclusive,
	}
}

// NewSessionManager is a helper to build SessionManager object.
func NewSessionManager(ctx context.Context, etcdAddress string, etcdPath string, self *Session) *SessionManager {
	etcdKV, err := initEtcd(etcdAddress, etcdPath)
	if err != nil {
		return nil
	}
	return &SessionManager{
		ctx:    ctx,
		etcdKV: etcdKV,
		Self:   self,
	}
}

// Init will initialize base struct in the SessionManager, including getServerID,
// and process keepAliveResponse
func (sm *SessionManager) Init() {
	sm.checkIDExist()
	serverID, err := sm.getServerID()
	if err != nil {
		panic(err)
	}
	sm.Self.ServerID = serverID
	ch, err := sm.registerService()
	if err != nil {
		panic(err)
	}
	sm.processKeepAliveResponse(ch)
}

// NewSession is a helper to build Session object.LeaseID will be assigned after
// registeration.
func NewSessionWithID(serverID int64, serverName, address string, exclusive bool) *Session {
	return &Session{
		ServerID:   serverID,
		ServerName: serverName,
		Address:    address,
		Exclusive:  exclusive,
	}
}

// GlobalServerID returns [singleton] ServerID.
// Before SetGlobalServerID, GlobalServerID() returns -1
func GlobalSessionManager() *SessionManager {
	return globalSessionManager
}

// SetGlobalServerID sets the [singleton] ServerID. ServerID returned by
// GlobalServerID(). Those who use GlobalServerID should call SetGlobalServerID()
// as early as possible in main() before use ServerID.
func SetGlobalSessionManager(sm *SessionManager) {
	globalSessionManager = sm
}

// GetServerID gets id from etcd with key: metaRootPath + "/services/id"
// Each server get ServerID and add one to id.
func (sm *SessionManager) getServerID() (int64, error) {
	return sm.getServerIDWithKey(defaultIDKey, defaultRetryTimes)
}

func (sm *SessionManager) checkIDExist() {
	sm.etcdKV.CompareVersionAndSwap(defaultServiceRoot+defaultIDKey, 0, "1")
}

func (sm *SessionManager) getServerIDWithKey(key string, retryTimes int) (int64, error) {
	res := int64(0)
	getServerIDWithKeyFn := func() error {
		value, err := sm.etcdKV.Load(defaultServiceRoot + key)
		if err != nil {
			return nil
		}
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Debug("session", zap.Error(err))
			return err
		}
		err = sm.etcdKV.CompareValueAndSwap(defaultServiceRoot+key, value,
			strconv.FormatInt(valueInt+1, 10))
		if err != nil {
			log.Debug("session", zap.Error(err))
			return err
		}
		res = valueInt
		return nil
	}

	err := retry.Retry(retryTimes, time.Millisecond*200, getServerIDWithKeyFn)
	return res, err
}

// RegisterService registers the service to etcd so that other services
// can find that the service is online and issue subsequent operations
// RegisterService will save a key-value in etcd
// key: metaRootPath + "/services" + "/ServerName-ServerID"
// value: json format
// {
//     "ServerID": "ServerID",
//     "ServerName": "ServerName",
//     "Address": "ip:port",
//     "LeaseID": "LeaseID",
// }
// MetaRootPath is configurable in the config file.
// Exclusive means whether this service can exist two at the same time, if so,
// it is false. Otherwise, set it to true.
func (sm *SessionManager) registerService() (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	respID, err := sm.etcdKV.Grant(defaultTTL)
	if err != nil {
		log.Error("register service", zap.Error(err))
		return nil, err
	}
	sm.Self.LeaseID = respID

	sessionJSON, err := json.Marshal(sm.Self)
	if err != nil {
		return nil, err
	}

	key := defaultServiceRoot + sm.Self.ServerName
	if !sm.Self.Exclusive {
		key = key + "-" + strconv.FormatInt(sm.Self.ServerID, 10)
	}
	err = sm.etcdKV.CompareVersionAndSwap(key, 0, string(sessionJSON), clientv3.WithLease(respID))
	if err != nil {
		fmt.Printf("compare and swap error %s\n. maybe the key has registered", err)
		return nil, err
	}

	ch, err := sm.etcdKV.KeepAlive(respID)
	if err != nil {
		fmt.Printf("keep alive error %s\n", err)
		return nil, err
	}
	return ch, nil
}

// ProcessKeepAliveResponse processes the response of etcd keepAlive interface
// If keepAlive fails for unexpected error, it will send a signal to the channel.
func (sm *SessionManager) processKeepAliveResponse(ch <-chan *clientv3.LeaseKeepAliveResponse) {
	go func() {
		for {
			select {
			case <-sm.ctx.Done():
				log.Error("keep alive", zap.Error(errors.New("context done")))
				return
			case resp, ok := <-ch:
				if !ok {
					panic("keepAlive with etcd failed")
				}
				if resp == nil {
					panic("keepAlive with etcd failed")
				}
			}
		}
	}()
}

// UpdateSessions will update local sessions same as the sessions saved in etcd.
// It makes locally stored sessions up-to-date.
func (sm *SessionManager) UpdateSessions(prefix string) error {
	resKey, resValue, err := sm.etcdKV.LoadWithPrefix(defaultServiceRoot + prefix)
	if err != nil {
		return err
	}
	for i := 0; i < len(resKey); i++ {
		session := &Session{}
		err = json.Unmarshal([]byte(resValue[i]), session)
		if err != nil {
			return err
		}
		sm.Sessions.Store(resKey[i], session)
	}
	return nil
}

// GetSessions gets all the services saved in memory.
// Before GetSessions, you should WatchServices or UpdateSessions first.
func (sm *SessionManager) GetSessions() map[string]*Session {
	sessions := map[string]*Session{}
	sm.Sessions.Range(func(key, value interface{}) bool {
		sessions[fmt.Sprint(key)] = value.(*Session)
		return true
	})
	return sessions
}

// WatchServices watch the service's up and down in etcd, and saves it into local
// sessions. If a server up, it will be add to sessions. But it won't get the
// sessions startup before watch start.
// UpdateSessions and WatchServices is recommended.
func (sm *SessionManager) WatchServices(ctx context.Context, prefix string) {
	rch := sm.etcdKV.WatchWithPrefix(defaultServiceRoot + prefix)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case wresp, ok := <-rch:
				if !ok {
					return
				}
				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						log.Debug("watch services",
							zap.Any("add kv", ev.Kv))
						session := &Session{}
						err := json.Unmarshal([]byte(ev.Kv.Value), session)
						if err != nil {
							log.Error("watch services", zap.Error(err))
							continue
						}
						sm.Sessions.Store(string(ev.Kv.Key), session)
					case mvccpb.DELETE:
						log.Debug("watch services",
							zap.Any("delete kv", ev.Kv))
						sm.Sessions.Delete(string(ev.Kv.Key))
					}
				}

			}
		}
	}()
}

func initEtcd(etcdAddress, rootPath string) (*etcdkv.EtcdKV, error) {
	var etcdKV *etcdkv.EtcdKV
	connectEtcdFn := func() error {
		etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		etcdKV = etcdkv.NewEtcdKV(etcdCli, rootPath)
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return nil, err
	}
	return etcdKV, nil
}
