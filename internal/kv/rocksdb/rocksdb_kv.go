// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rocksdbkv

import (
	"github.com/tecbot/gorocksdb"
)

type RocksdbKV struct {
	opts         *gorocksdb.Options
	db           *gorocksdb.DB
	writeOptions *gorocksdb.WriteOptions
	readOptions  *gorocksdb.ReadOptions
	name         string
}

func NewRocksdbKV(name string) (*RocksdbKV, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	wo := gorocksdb.NewDefaultWriteOptions()
	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}
	return &RocksdbKV{
		opts:         opts,
		db:           db,
		writeOptions: wo,
		readOptions:  ro,
		name:         name,
	}, nil
}

func (kv *RocksdbKV) Close() {
	kv.db.Close()
}

func (kv *RocksdbKV) GetName() string {
	return kv.name
}

func (kv *RocksdbKV) Load(key string) (string, error) {
	value, err := kv.db.Get(kv.readOptions, []byte(key))
	defer value.Free()
	return string(value.Data()), err
}

func (kv *RocksdbKV) LoadWithPrefix(key string) ([]string, []string, error) {
	kv.readOptions.SetPrefixSameAsStart(true)
	kv.db.Close()
	kv.opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(len(key)))
	var err error
	kv.db, err = gorocksdb.OpenDb(kv.opts, kv.GetName())
	if err != nil {
		return nil, nil, err
	}

	iter := kv.db.NewIterator(kv.readOptions)
	defer iter.Close()
	keys := make([]string, 0)
	values := make([]string, 0)
	iter.Seek([]byte(key))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, string(key.Data()))
		values = append(values, string(value.Data()))
		key.Free()
		value.Free()
	}
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

func (kv *RocksdbKV) MultiLoad(keys []string) ([]string, error) {
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		value, err := kv.db.Get(kv.readOptions, []byte(key))
		if err != nil {
			return []string{}, err
		}
		values = append(values, string(value.Data()))
	}
	return values, nil
}

func (kv *RocksdbKV) Save(key, value string) error {
	err := kv.db.Put(kv.writeOptions, []byte(key), []byte(value))
	return err
}

func (kv *RocksdbKV) MultiSave(kvs map[string]string) error {
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Clear()
	for k, v := range kvs {
		writeBatch.Put([]byte(k), []byte(v))
	}
	err := kv.db.Write(kv.writeOptions, writeBatch)
	return err
}

func (kv *RocksdbKV) RemoveWithPrefix(prefix string) error {
	kv.readOptions.SetPrefixSameAsStart(true)
	kv.db.Close()
	kv.opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(len(prefix)))
	var err error
	kv.db, err = gorocksdb.OpenDb(kv.opts, kv.GetName())
	if err != nil {
		return err
	}

	iter := kv.db.NewIterator(kv.readOptions)
	defer iter.Close()
	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		err := kv.db.Delete(kv.writeOptions, key.Data())
		if err != nil {
			return nil
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

func (kv *RocksdbKV) Remove(key string) error {
	err := kv.db.Delete(kv.writeOptions, []byte(key))
	return err
}

func (kv *RocksdbKV) MultiRemove(keys []string) error {
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Clear()
	for _, key := range keys {
		writeBatch.Delete([]byte(key))
	}
	err := kv.db.Write(kv.writeOptions, writeBatch)
	return err
}

func (kv *RocksdbKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Clear()
	for k, v := range saves {
		writeBatch.Put([]byte(k), []byte(v))
	}
	for _, key := range removals {
		writeBatch.Delete([]byte(key))
	}
	err := kv.db.Write(kv.writeOptions, writeBatch)
	return err
}

func (kv *RocksdbKV) MultiRemoveWithPrefix(keys []string) error {
	panic("not implement")
}
func (kv *RocksdbKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	panic("not implement")
}
