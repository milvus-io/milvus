package typeutil

type OrderedMap[K comparable, V any] struct {
	keys   []K
	values map[K]V
}

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	o := OrderedMap[K, V]{}
	o.keys = []K{}
	o.values = map[K]V{}
	return &o
}

func (o *OrderedMap[K, V]) Get(key K) (V, bool) {
	val, exists := o.values[key]
	return val, exists
}

func (o *OrderedMap[K, V]) Set(key K, value V) {
	_, exists := o.values[key]
	if !exists {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

func (o *OrderedMap[K, V]) Delete(key K) {
	// check key is in use
	_, ok := o.values[key]
	if !ok {
		return
	}
	// remove from keys
	for i, k := range o.keys {
		if k == key {
			o.keys = append(o.keys[:i], o.keys[i+1:]...)
			break
		}
	}
	// remove from values
	delete(o.values, key)
}

func (o *OrderedMap[K, V]) Keys() []K {
	return o.keys
}

// SortKeys Sort the map keys using your sort func
func (o *OrderedMap[K, V]) SortKeys(sortFunc func(keys []K)) {
	sortFunc(o.keys)
}
