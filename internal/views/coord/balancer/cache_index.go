package balancer

import (
	"encoding/binary"

	iradix "github.com/hashicorp/go-immutable-radix"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// immutableIndex copies only radix paths on writes. Its zero value is empty;
// readers can retain any version while a writer publishes a successor.
type immutableIndex[T any] struct{ tree *iradix.Tree }

func (i immutableIndex[T]) get(key []byte) (T, bool) {
	if i.tree != nil {
		if value, ok := i.tree.Get(key); ok {
			return value.(T), true
		}
	}
	var zero T
	return zero, false
}

func (i immutableIndex[T]) set(key []byte, value T) immutableIndex[T] {
	tree := i.tree
	if tree == nil {
		tree = iradix.New()
	}
	next, _, _ := tree.Insert(key, value)
	return immutableIndex[T]{tree: next}
}

func (i immutableIndex[T]) remove(key []byte) immutableIndex[T] {
	if i.tree == nil {
		return i
	}
	next, _, _ := i.tree.Delete(key)
	return immutableIndex[T]{tree: next}
}

func (i immutableIndex[T]) each(fn func(T) bool) {
	if i.tree != nil {
		i.tree.Root().Walk(func(_ []byte, value interface{}) bool { return !fn(value.(T)) })
	}
}

func (i immutableIndex[T]) len() int {
	if i.tree == nil {
		return 0
	}
	return i.tree.Len()
}

func idKey(id int64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(id))
	return key[:]
}
func shardKey(id qviews.ShardID) []byte { return append(idKey(id.ReplicaID), id.VChannel...) }
