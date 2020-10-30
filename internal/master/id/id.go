// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package id

import (
	"github.com/zilliztech/milvus-distributed/internal/master/tso"
)


// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalIdAllocator struct {
	allocator tso.Allocator
}

func NewGlobalIdAllocator() *GlobalIdAllocator {
	return &GlobalIdAllocator{
		allocator: tso.NewGlobalTSOAllocator("idTimestamp"),
	}
}

// Initialize will initialize the created global TSO allocator.
func (gia *GlobalIdAllocator) Initialize() error {
	return gia.allocator.Initialize()
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gia *GlobalIdAllocator) Generate(count uint32) (int64, int64, error) {
	timestamp, err:= gia.allocator.GenerateTSO(count)
	if err != nil{
		return 0, 0, err
	}
	idStart := int64(timestamp)
	idEnd := idStart + int64(count)
	return idStart, idEnd, nil
}

