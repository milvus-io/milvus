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

package allocator

import (
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// GIDAllocator is interface for GlobalIDAllocator.
// Alloc allocates the id of the count number.
// AllocOne allocates one id.
// UpdateID update timestamp of allocator.
type GIDAllocator interface {
	Alloc(count uint32) (UniqueID, UniqueID, error)
	AllocOne() (UniqueID, error)
	UpdateID() error
}

// GlobalIDAllocator is the global single point TSO allocator.
type GlobalIDAllocator struct {
	allocator tso.Allocator
}

// NewGlobalIDAllocator creates GlobalIDAllocator for allocates ID.
func NewGlobalIDAllocator(key string, base kv.TxnKV) *GlobalIDAllocator {
	allocator := tso.NewGlobalTSOAllocator(key, base)
	allocator.SetLimitMaxLogic(false)
	return &GlobalIDAllocator{
		allocator: allocator,
	}
}

// Initialize will initialize the created global TSO allocator.
func (gia *GlobalIDAllocator) Initialize() error {
	return gia.allocator.Initialize()
}

// Alloc allocates the id of the count number.
// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gia *GlobalIDAllocator) Alloc(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(count)
	if err != nil {
		return 0, 0, err
	}
	idEnd := typeutil.UniqueID(timestamp) + 1
	idStart := idEnd - int64(count)
	return idStart, idEnd, nil
}

// AllocOne allocates one id.
func (gia *GlobalIDAllocator) AllocOne() (typeutil.UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}
	idStart := typeutil.UniqueID(timestamp)
	return idStart, nil
}

// UpdateID update timestamp of allocator.
func (gia *GlobalIDAllocator) UpdateID() error {
	return gia.allocator.UpdateTSO()
}
