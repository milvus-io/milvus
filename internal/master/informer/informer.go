package informer

type Informer interface {
	Listener(key interface{}) (interface{}, error)
}
