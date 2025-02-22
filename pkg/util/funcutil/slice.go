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

package funcutil

import (
	"reflect"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SliceContain returns true if slice s contains item.
func SliceContain[T comparable](s []T, item T) bool {
	for i := range s {
		if s[i] == item {
			return true
		}
	}
	return false
}

// SliceSetEqual is used to compare two Slice
func SliceSetEqual[T comparable](s1, s2 []T) bool {
	if len(s1) != len(s2) {
		return false
	}

	set := typeutil.NewSet(s1...)
	for i := range s2 {
		if !set.Contain(s2[i]) {
			return false
		}
	}
	return true
}

// SortedSliceEqual is used to compare two Sorted Slice
func SortedSliceEqual(s1, s2 interface{}) bool {
	ss1 := reflect.ValueOf(s1)
	ss2 := reflect.ValueOf(s2)
	if ss1.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss2.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss1.Len() != ss2.Len() {
		return false
	}
	for i := 0; i < ss1.Len(); i++ {
		if ss2.Index(i).Interface() != ss1.Index(i).Interface() {
			return false
		}
	}
	return true
}
