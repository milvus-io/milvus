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

package milvusclient

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/client/v3/roaringfilter"
)

// RoaringBitmapBlob is a client-built exact integer membership bitmap for
// roaring_match(field, {bitmap}). It travels as a native protobuf bytes
// template value and contains an MRB1 envelope around portable Roaring64 data.
type RoaringBitmapBlob []byte

type signedInteger interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

func widenSignedIntegers[T signedInteger](members []T) []int64 {
	values := make([]int64, len(members))
	for i, member := range members {
		values[i] = int64(member)
	}
	return values
}

// NewRoaringBitmapBlob builds an exact membership bitmap from a slice of
// signed integers. Supported input types are []int, []int8, []int16, []int32,
// and []int64; values are sign-extended to int64 before serialization.
func NewRoaringBitmapBlob(members any) (RoaringBitmapBlob, error) {
	var values []int64
	switch typed := members.(type) {
	case []int:
		values = widenSignedIntegers(typed)
	case []int8:
		values = widenSignedIntegers(typed)
	case []int16:
		values = widenSignedIntegers(typed)
	case []int32:
		values = widenSignedIntegers(typed)
	case []int64:
		values = typed
	default:
		return nil, errors.Errorf(
			"roaring bitmap members must be []int, []int8, []int16, []int32, or []int64, got %T", members)
	}
	blob, err := roaringfilter.Build(values)
	if err != nil {
		return nil, err
	}
	return RoaringBitmapBlob(blob), nil
}
