package kv

import "go.etcd.io/etcd/clientv3"

type Base interface {
	Load(key string) (string, error)
	Save(key, value string) error
	Remove(key string) error
	Watch(key string) clientv3.WatchChan
	WatchWithPrefix(key string) clientv3.WatchChan
	LoadWithPrefix(key string) ( []string, []string)
}
