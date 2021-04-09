package kv

type Base interface {
	Load(key string) (string, error)
	MultiLoad(keys []string) ([]string, error)
	LoadWithPrefix(key string) ([]string, []string, error)
	Save(key, value string) error
	MultiSave(kvs map[string]string) error
	Remove(key string) error
	MultiRemove(keys []string) error
	RemoveWithPrefix(key string) error

	Close()
}

type TxnBase interface {
	Base
	MultiSaveAndRemove(saves map[string]string, removals []string) error
	MultiRemoveWithPrefix(keys []string) error
	MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error
}
