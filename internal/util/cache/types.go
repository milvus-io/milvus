package cache

type Key interface {
}
type Value interface {
}

type Entry struct {
	Key
	Value
	Size int
}

type Cache interface {
	Add(Key, Value)
	Get(key Key) (value Value, ok bool)
	Remove(key Key)
	Contains(key Key) bool
	Purge()
	Close()
}
