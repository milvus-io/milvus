package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const default_ttl = 10
const default_retry_times = 3
const default_ID_key = "services/id"

type Session struct {
	ServerID   int64
	ServerName string
	Address    string
	LeaseID    clientv3.LeaseID
}

// NewSession is a helper to build Session object.LeaseID will be assigned after
// registeration.
func NewSession(serverID int64, serverName, address string) *Session {
	return &Session{
		ServerID:   serverID,
		ServerName: serverName,
		Address:    address,
	}
}

// GetServerID gets id from etcd with key: metaRootPath + "/services/id"
// Each server get ServerID and add one to id.
func GetServerID(etcd *etcdkv.EtcdKV) (int64, error) {
	return getServerIDWithKey(etcd, default_ID_key)
}

// GetServerID gets id from etcd with key: metaRootPath + key
// Each server get ServerID and add one to id.
func getServerIDWithKey(etcd *etcdkv.EtcdKV, key string) (int64, error) {
	res := int64(-1)
	getServerIDWithKeyFn := func() error {
		value, err := etcd.Load(key)
		log.Debug("session", zap.String("get serverid", value))
		if err != nil {
			err = etcd.CompareVersionAndSwap(key, 0, "1")
			if err != nil {
				log.Debug("session", zap.Error(err))
				return err
			}
			res = 0
			return nil
		}
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Debug("session", zap.Error(err))
			return err
		}
		err = etcd.CompareValueAndSwap(key, value,
			strconv.FormatInt(valueInt+1, 10))
		if err != nil {
			log.Debug("session", zap.Error(err))
			return err
		}
		res = valueInt
		return nil
	}

	err := retry.Retry(30, time.Millisecond*200, getServerIDWithKeyFn)
	return res, err
}

// RegisterService registers the service to etcd so that other services
// can find that the service is online and issue subsequent operations
// RegisterService will save a key-value in etcd
// key: metaRootPath + "/services" + "/ServerName-ServerID"
// value: json format
// {
//     "ServerID": ServerID
//     "ServerName": ServerName // ServerName
//     "Address": ip:port // Address of service, including ip and port
//     "LeaseID": LeaseID // The ID of etcd lease
// }
// MetaRootPath is configurable in the config file.
func RegisterService(etcdKV *etcdkv.EtcdKV, session *Session, ttl int64) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	respID, err := etcdKV.Grant(ttl)
	if err != nil {
		log.Error("register service", zap.Error(err))
		return nil, err
	}
	session.LeaseID = respID

	sessionJSON, err := json.Marshal(session)
	if err != nil {
		return nil, err
	}

	err = etcdKV.SaveWithLease(fmt.Sprintf("/services/%s-%d", session.ServerName, session.ServerID),
		string(sessionJSON), respID)
	if err != nil {
		fmt.Printf("put lease error %s\n", err)
		return nil, err
	}

	ch, err := etcdKV.KeepAlive(respID)
	if err != nil {
		fmt.Printf("keep alive error %s\n", err)
		return nil, err
	}
	return ch, nil
}

// ProcessKeepAliveResponse processes the response of etcd keepAlive interface
// If keepAlive fails for unexpected error, it will retry for default_retry_times times
func ProcessKeepAliveResponse(ctx context.Context, ch <-chan *clientv3.LeaseKeepAliveResponse) (signal <-chan bool) {
	signalOut := make(chan bool)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Error("keep alive", zap.Error(errors.New("context done")))
				return
			case resp, ok := <-ch:
				if !ok {
					signalOut <- false
				}
				if resp != nil {
					signalOut <- true
				} else {
					signalOut <- false
				}
			}
		}
	}()
	return signalOut
}

// GetAllSessions gets all the services registered in etcd.
// This gets all the key with prefix metaRootPath + "/services/" + prefix
// For general, "datanode" to get all datanodes
func GetSessions(etcdKV *etcdkv.EtcdKV, prefix string) ([]*Session, error) {
	sessions := make([]*Session, 0)
	_, resValue, err := etcdKV.LoadWithPrefix("/services/" + prefix)
	if err != nil {
		return nil, err
	}
	for _, value := range resValue {
		session := &Session{}
		err = json.Unmarshal([]byte(value), session)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

// WatchServices watch all events in etcd.
// If a server register, a session will be sent to addChannel
// If a server offline, a session will be sent to deleteChannel
func WatchServices(ctx context.Context, etcdKV *etcdkv.EtcdKV, prefix string) (addChannel <-chan *Session, deleteChannel <-chan *Session) {
	addCh := make(chan *Session, 10)
	deleteCh := make(chan *Session, 10)
	rch := etcdKV.WatchWithPrefix("/services/" + prefix)
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
					session := &Session{}
					err := json.Unmarshal([]byte(ev.Kv.Value), session)
					if err != nil {
						log.Error("watch services", zap.Error(err))
						continue
					}
					switch ev.Type {
					case mvccpb.PUT:
						log.Debug("watch services",
							zap.Any("addchannel kv", ev.Kv))
						addCh <- session
					case mvccpb.DELETE:
						log.Debug("watch services",
							zap.Any("deletechannel kv", ev.Kv))
						deleteCh <- session
					}
				}

			}
		}
	}()
	return addCh, deleteCh
}
