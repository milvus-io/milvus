package kv

type Base interface {
	Load(key string) (string, error)
	Save(key, value string) error
	Remove(key string) error
}
