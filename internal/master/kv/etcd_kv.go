package kv

import (
	"context"
	"path"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/util/etcdutil"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	requestTimeout  = 10 * time.Second
	slowRequestTime = 1 * time.Second
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

type EtcdKVBase struct {
	client   *clientv3.Client
	rootPath string
}

// NewEtcdKVBase creates a new etcd kv.
func NewEtcdKVBase(client *clientv3.Client, rootPath string) *EtcdKVBase {
	return &EtcdKVBase{
		client:   client,
		rootPath: rootPath,
	}
}

func (kv *EtcdKVBase) Close() {
	kv.client.Close()
}

func (kv *EtcdKVBase) LoadWithPrefix(key string) ([]string, []string) {
	key = path.Join(kv.rootPath, key)
	println("in loadWithPrefix,", key)
	resp, err := etcdutil.EtcdKVGet(kv.client, key, clientv3.WithPrefix())
	if err != nil {
		return []string{}, []string{}
	}
	var keys []string
	var values []string
	for _, kvs := range resp.Kvs {
		//println(len(kvs.))
		if len(kvs.Key) <= 0 {
			println("KKK")
			continue
		}
		keys = append(keys, string(kvs.Key))
		values = append(values, string(kvs.Value))
	}
	//println(keys)
	//println(values)
	return keys, values
}

func (kv *EtcdKVBase) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	resp, err := etcdutil.EtcdKVGet(kv.client, key)
	if err != nil {
		return "", err
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *EtcdKVBase) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)

	txn := NewSlowLogTxn(kv.client)
	resp, err := txn.Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		log.Error("save to etcd meet error", zap.String("key", key), zap.String("value", value))
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.WithStack(errTxnFailed)
	}
	return nil
}

func (kv *EtcdKVBase) Remove(key string) error {
	key = path.Join(kv.rootPath, key)

	txn := NewSlowLogTxn(kv.client)
	resp, err := txn.Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		log.Error("remove from etcd meet error", zap.String("key", key))
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.WithStack(errTxnFailed)
	}
	return nil
}

func (kv *EtcdKVBase) Watch(key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key)
	return rch
}

func (kv *EtcdKVBase) WatchWithPrefix(key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithPrefix())
	return rch
}

// SlowLogTxn wraps etcd transaction and log slow one.
type SlowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

// NewSlowLogTxn create a SlowLogTxn.
func NewSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	return &SlowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

// If takes a list of comparison. If all comparisons passed in succeed,
// the operations passed into Then() will be executed. Or the operations
// passed into Else() will be executed.
func (t *SlowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &SlowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

// Then takes a list of operations. The Ops list will be executed, if the
// comparisons passed in If() succeed.
func (t *SlowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &SlowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *SlowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Since(start)
	if cost > slowRequestTime {
		log.Warn("txn runs too slow",
			zap.Error(err),
			zap.Reflect("response", resp),
			zap.Duration("cost", cost))
	}
	//label := "success"
	//if err != nil {
	//	label = "failed"
	//}
	//txnCounter.WithLabelValues(label).Inc()
	//txnDuration.WithLabelValues(label).Observe(cost.Seconds())
	return resp, errors.WithStack(err)
}
