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

package row

import (
	"reflect"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var cachedCandidates *typeutil.ConcurrentMap[reflect.Type, *ReceiverCandidate]

func init() {
	cachedCandidates = typeutil.NewConcurrentMap[reflect.Type, *ReceiverCandidate]()
}

func GetReceiverCandidate(v reflect.Type) *ReceiverCandidate {
	rc, ok := cachedCandidates.Get(v)
	if ok {
		return rc
	}

	// reflect.Type.String() cannot work as unique identifier for singleflight
	// accept multiple parse for now
	rc = &ReceiverCandidate{
		name2Index: parseCandidate(v),
	}
	cachedCandidates.Insert(v, rc)

	return rc
}
