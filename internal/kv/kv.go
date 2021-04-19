package kv

import "go.etcd.io/etcd/clientv3"

type Base interface {
	Load(key string) (string, error)
	MultiLoad(keys []string) ([]string, error)
	Save(key, value string) error
	MultiSave(kvs map[string]string) error
	Remove(key string) error
	MultiRemove(keys []string) error
	Watch(key string) clientv3.WatchChan
	MultiSaveAndRemove(saves map[string]string, removals []string) error
	WatchWithPrefix(key string) clientv3.WatchChan
	LoadWithPrefix(key string) ([]string, []string, error)
	Close()
}
