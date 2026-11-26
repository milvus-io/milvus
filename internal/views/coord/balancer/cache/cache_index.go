package cache

import (
	"encoding/binary"
	"iter"

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

func (i immutableIndex[T]) getShard(id qviews.ShardID) (T, bool) {
	// Keep ordinary channel names on the stack without imposing a length limit
	// or retaining a scratch buffer. Only oversized lookups allocate.
	var buffer [128]byte
	size := 8 + len(id.VChannel)
	key := buffer[:]
	if size > len(buffer) {
		key = make([]byte, size)
	}
	key = key[:size]
	putShardKey(key, id)
	return i.get(key)
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

// RangeReplicaShards walks the replica prefix of the immutable resident shard index.
func (c *CollectionEntry) RangeReplicaShards(replica int64) iter.Seq[qviews.ShardID] {
	return func(yield func(qviews.ShardID) bool) {
		key := idKey(replica)
		if c.shards.tree != nil {
			c.shards.tree.Root().WalkPrefix(key[:], func(_ []byte, value interface{}) bool {
				return !yield(value.(*ShardEntry).id)
			})
		}
	}
}

func (i immutableIndex[T]) len() int {
	if i.tree == nil {
		return 0
	}
	return i.tree.Len()
}

// Fixed-size keys are returned by value so lookups can keep their buffers on
// the stack. Insert retains the slice, so its array must not be reused for a
// different key.
func idKey(id int64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(id))
	return key
}

func shardKey(id qviews.ShardID) []byte {
	key := make([]byte, 8+len(id.VChannel))
	putShardKey(key, id)
	return key
}

func putShardKey(key []byte, id qviews.ShardID) {
	binary.BigEndian.PutUint64(key[:8], uint64(id.ReplicaID))
	copy(key[8:], id.VChannel)
}
