// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pebblekv

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var _ kv.BaseKV = (*PebbleKV)(nil)

// PebbleKV is KV implemented by pebble
type PebbleKV struct {
	Opts         *pebble.Options
	DB           *pebble.DB
	WriteOptions *pebble.WriteOptions
	ReadOptions  *pebble.IterOptions
	name         string
}

const (
	// LRUCacheSize is the lru cache size of pebble, default 0
	LRUCacheSize = 0
)

// NewPebbleKV returns a pebblekv object, only used in test
func NewPebbleKV(name string) (*PebbleKV, error) {
	if name == "" {
		return nil, errors.New("pebble name is nil")
	}
	c := pebble.NewCache(LRUCacheSize)
	defer c.Unref()
	opts := pebble.Options{Cache: c}

	wo := pebble.WriteOptions{}
	ro := pebble.IterOptions{}
	db, err := pebble.Open(name, &opts)
	if err != nil {
		return nil, err
	}
	return &PebbleKV{
		Opts:         &opts,
		DB:           db,
		WriteOptions: &wo,
		ReadOptions:  &ro,
		name:         name,
	}, nil
}

// Close free resource of pebble
func (kv *PebbleKV) Close() {
	if kv.DB != nil {
		kv.DB.Close()
	}
}

// GetName returns the name of this object
func (kv *PebbleKV) GetName() string {
	return kv.name
}

// Load returns the value of specified key
func (kv *PebbleKV) Load(key string) (string, error) {
	if kv.DB == nil {
		return "", fmt.Errorf("pebble instance is nil when load %s", key)
	}
	if key == "" {
		return "", errors.New("pebble kv does not support load empty key")
	}
	value, closer, err := kv.DB.Get([]byte(key))
	if err != nil && err != pebble.ErrNotFound {
		return "", err
	}
	if closer != nil {
		defer closer.Close()
	}
	return string(value), nil
}

// LoadWithPrefix returns a batch values of keys with a prefix
// if prefix is "", then load every thing from the database
func (kv *PebbleKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	if kv.DB == nil {
		return nil, nil, fmt.Errorf("pebble instance is nil when load %s", prefix)
	}
	option := pebble.IterOptions{
		UpperBound: []byte(typeutil.AddOne(prefix)),
	}
	iter := NewPebbleIteratorWithUpperBound(kv.DB, &option)
	defer iter.Close()

	var keys, values []string
	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, string(key))
		values = append(values, string(value))
	}
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

// MultiLoad load a batch of values by keys
func (kv *PebbleKV) MultiLoad(keys []string) ([]string, error) {
	if kv.DB == nil {
		return nil, errors.New("pebble instance is nil when do MultiLoad")
	}
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		value, closer, err := kv.DB.Get([]byte(key))
		if err != nil && err != pebble.ErrNotFound {
			return nil, err
		}
		if closer != nil {
			defer closer.Close()
		}
		values = append(values, string(value))
	}
	return values, nil
}

// Save a pair of key-value
func (kv *PebbleKV) Save(key, value string) error {
	if kv.DB == nil {
		return errors.New("pebble instance is nil when do save")
	}
	if key == "" {
		return errors.New("pebble kv does not support empty key")
	}
	if value == "" {
		return errors.New("pebble kv does not support empty value")
	}

	return kv.DB.Set([]byte(key), []byte(value), kv.WriteOptions)
}

// MultiSave a batch of key-values
func (kv *PebbleKV) MultiSave(kvs map[string]string) error {
	if kv.DB == nil {
		return errors.New("pebble instance is nil when do MultiSave")
	}

	writeBatch := kv.DB.NewBatch()
	defer writeBatch.Close()

	for k, v := range kvs {
		writeBatch.Set([]byte(k), []byte(v), kv.WriteOptions)
	}
	return writeBatch.Commit(kv.WriteOptions)
}

// RemoveWithPrefix removes a batch of key-values with specified prefix
// If prefix is "", then all data in the pebble kv will be deleted
func (kv *PebbleKV) RemoveWithPrefix(prefix string) error {
	if kv.DB == nil {
		return errors.New("pebble instance is nil when do RemoveWithPrefix")
	}
	if len(prefix) == 0 {
		// better to use drop column family, but as we use default column family, we just delete ["",lastKey+1)
		readOpts := pebble.IterOptions{}
		iter := NewPebbleIterator(kv.DB, &readOpts)
		defer iter.Close()
		// seek to the last key
		iter.SeekToLast()
		if iter.Valid() {
			return kv.DeleteRange(prefix, typeutil.AddOne(string(iter.Key())))
		}
		// nothing in the range, skip
		return nil
	}
	prefixEnd := typeutil.AddOne(prefix)
	return kv.DeleteRange(prefix, prefixEnd)
}

// Remove is used to remove a pair of key-value
func (kv *PebbleKV) Remove(key string) error {
	if kv.DB == nil {
		return errors.New("pebble instance is nil when do Remove")
	}
	if key == "" {
		return errors.New("pebble kv does not support empty key")
	}
	err := kv.DB.Delete([]byte(key), kv.WriteOptions)
	return err
}

// MultiRemove is used to remove a batch of key-values
func (kv *PebbleKV) MultiRemove(keys []string) error {
	if kv.DB == nil {
		return errors.New("pebble instance is nil when do MultiRemove")
	}
	writeBatch := kv.DB.NewBatch()
	for _, key := range keys {
		writeBatch.Delete([]byte(key), kv.WriteOptions)
	}
	return writeBatch.Commit(kv.WriteOptions)
}

// MultiSaveAndRemove provides a transaction to execute a batch of operations
func (kv *PebbleKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	if kv.DB == nil {
		return errors.New("Pebble instance is nil when do MultiSaveAndRemove")
	}
	writeBatch := kv.DB.NewBatch()
	for k, v := range saves {
		writeBatch.Set([]byte(k), []byte(v), kv.WriteOptions)
	}
	for _, key := range removals {
		writeBatch.Delete([]byte(key), kv.WriteOptions)
	}
	return writeBatch.Commit(kv.WriteOptions)
}

// DeleteRange remove a batch of key-values from startKey to endKey
func (kv *PebbleKV) DeleteRange(startKey, endKey string) error {
	if kv.DB == nil {
		return errors.New("Pebble instance is nil when do DeleteRange")
	}
	if startKey >= endKey {
		return fmt.Errorf("PebbleKV delete range startkey must < endkey, startkey %s, endkey %s", startKey, endKey)
	}
	writeBatch := kv.DB.NewBatch()
	writeBatch.DeleteRange([]byte(startKey), []byte(endKey), kv.WriteOptions)
	return writeBatch.Commit(kv.WriteOptions)
}

// MultiRemoveWithPrefix is used to remove a batch of key-values with the same prefix
func (kv *PebbleKV) MultiRemoveWithPrefix(keys []string) error {
	panic("not implement")
}

// MultiSaveAndRemoveWithPrefix is used to execute a batch operators with the same prefix
func (kv *PebbleKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	panic("not implement")
}
