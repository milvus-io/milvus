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

package segment

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// allocPool pool of Allocation, to reduce allocation of Allocation
var allocPool = sync.Pool{
	New: func() interface{} {
		return &Allocation{}
	},
}

// GetAllocation unifies way to retrieve allocation struct
func GetAllocation(numOfRows int64) *Allocation {
	v := allocPool.Get()
	a, ok := v.(*Allocation)
	if !ok {
		a = &Allocation{}
	}
	if a == nil {
		return &Allocation{
			NumOfRows: numOfRows,
		}
	}
	a.NumOfRows = numOfRows
	a.ExpireTime = 0
	a.SegmentID = 0
	return a
}

// PutAllocation puts an allocation for recycling
func PutAllocation(a *Allocation) {
	allocPool.Put(a)
}

// Allocation records the allocation info
type Allocation struct {
	SegmentID  typeutil.UniqueID
	NumOfRows  int64
	ExpireTime typeutil.Timestamp
}

func (alloc *Allocation) String() string {
	t, _ := tsoutil.ParseTS(alloc.ExpireTime)
	return fmt.Sprintf("SegmentID: %d, NumOfRows: %d, ExpireTime: %v", alloc.SegmentID, alloc.NumOfRows, t)
}
