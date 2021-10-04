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

package funcutil

import "reflect"

// SliceContain returns true if slice s contains item.
func SliceContain(s interface{}, item interface{}) bool {
	ss := reflect.ValueOf(s)
	if ss.Kind() != reflect.Slice {
		panic("SliceContain expect a slice")
	}

	for i := 0; i < ss.Len(); i++ {
		if ss.Index(i).Interface() == item {
			return true
		}
	}

	return false
}

// SliceSetEqual is used to compare two Slice
func SliceSetEqual(s1 interface{}, s2 interface{}) bool {
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
		if !SliceContain(s2, ss1.Index(i).Interface()) {
			return false
		}
	}
	return true
}

// SortedSliceEqual is used to compare two Sorted Slice
func SortedSliceEqual(s1 interface{}, s2 interface{}) bool {
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
