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

package storage

import (
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

var (
	// ErrNoMoreRecord is the error that the iterator does not have next record.
	ErrNoMoreRecord = errors.New("no more record")
	// ErrDisposed is the error that the iterator is disposed.
	ErrDisposed = errors.New("iterator is disposed")
)

// Iterator is the iterator interface.
type Iterator interface {
	// HasNext returns true if the iterator have unread record
	HasNext() bool
	// Next returns the next record
	Next() (interface{}, error)
	// Dispose disposes the iterator
	Dispose()
}

// Value is the return value of Next
type Value struct {
	ID        int64
	PK        PrimaryKey
	Timestamp int64
	IsDeleted bool
	Value     interface{}
}

// InsertBinlogIterator is the iterator of binlog
type InsertBinlogIterator struct {
	dispose   int32 // 0: false, 1: true
	data      *InsertData
	PKfieldID int64
	PkType    schemapb.DataType
	pos       int
}

// NewInsertBinlogIterator creates a new iterator
//
// Deprecated: use storage.NewBinlogDeserializeReader instead
func NewInsertBinlogIterator(blobs []*Blob, PKfieldID UniqueID, pkType schemapb.DataType) (*InsertBinlogIterator, error) {
	// TODO: load part of file to read records other than loading all content
	reader := NewInsertCodecWithSchema(nil)

	_, _, serData, err := reader.Deserialize(blobs)
	if err != nil {
		return nil, err
	}

	return &InsertBinlogIterator{data: serData, PKfieldID: PKfieldID, PkType: pkType}, nil
}

// HasNext returns true if the iterator have unread record
func (itr *InsertBinlogIterator) HasNext() bool {
	return !itr.isDisposed() && itr.hasNext()
}

// Next returns the next record
func (itr *InsertBinlogIterator) Next() (interface{}, error) {
	if itr.isDisposed() {
		return nil, ErrDisposed
	}

	if !itr.hasNext() {
		return nil, ErrNoMoreRecord
	}

	m := make(map[FieldID]interface{})
	for fieldID, fieldData := range itr.data.Data {
		m[fieldID] = fieldData.GetRow(itr.pos)
	}
	pk, err := GenPrimaryKeyByRawData(itr.data.Data[itr.PKfieldID].GetRow(itr.pos), itr.PkType)
	if err != nil {
		return nil, err
	}

	v := &Value{
		ID:        itr.data.Data[common.RowIDField].GetRow(itr.pos).(int64),
		Timestamp: itr.data.Data[common.TimeStampField].GetRow(itr.pos).(int64),
		PK:        pk,
		IsDeleted: false,
		Value:     m,
	}
	itr.pos++
	return v, nil
}

// Dispose disposes the iterator
func (itr *InsertBinlogIterator) Dispose() {
	atomic.CompareAndSwapInt32(&itr.dispose, 0, 1)
}

func (itr *InsertBinlogIterator) hasNext() bool {
	_, ok := itr.data.Data[common.RowIDField]
	if !ok {
		return false
	}
	return itr.pos < itr.data.Data[common.RowIDField].RowNum()
}

func (itr *InsertBinlogIterator) isDisposed() bool {
	return atomic.LoadInt32(&itr.dispose) == 1
}
