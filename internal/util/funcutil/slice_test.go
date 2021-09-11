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

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SliceContain(t *testing.T) {
	invalid := "invalid"
	assert.Panics(t, func() { SliceContain(invalid, 1) })

	strSlice := []string{"test", "for", "SliceContain"}
	intSlice := []int{1, 2, 3}

	cases := []struct {
		s    interface{}
		item interface{}
		want bool
	}{
		{strSlice, "test", true},
		{strSlice, "for", true},
		{strSlice, "SliceContain", true},
		{strSlice, "tests", false},
		{intSlice, 1, true},
		{intSlice, 2, true},
		{intSlice, 3, true},
		{intSlice, 4, false},
	}

	for _, test := range cases {
		if got := SliceContain(test.s, test.item); got != test.want {
			t.Errorf("SliceContain(%v, %v) = %v", test.s, test.item, test.want)
		}
	}
}

func Test_SliceSetEqual(t *testing.T) {
	invalid := "invalid"
	assert.Panics(t, func() { SliceSetEqual(invalid, 1) })
	temp := []int{1, 2, 3}
	assert.Panics(t, func() { SliceSetEqual(temp, invalid) })

	cases := []struct {
		s1   interface{}
		s2   interface{}
		want bool
	}{
		{[]int{}, []int{}, true},
		{[]string{}, []string{}, true},
		{[]int{1, 2, 3}, []int{3, 2, 1}, true},
		{[]int{1, 2, 3}, []int{1, 2, 3}, true},
		{[]int{1, 2, 3}, []int{}, false},
		{[]int{1, 2, 3}, []int{1, 2}, false},
		{[]int{1, 2, 3}, []int{4, 5, 6}, false},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"SliceSetEqual", "test", "for"}, true},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"test", "for", "SliceSetEqual"}, true},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"test", "for"}, false},
		{[]string{"test", "for", "SliceSetEqual"}, []string{}, false},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"test", "for", "SliceContain"}, false},
	}

	for _, test := range cases {
		if got := SliceSetEqual(test.s1, test.s2); got != test.want {
			t.Errorf("SliceSetEqual(%v, %v) = %v", test.s1, test.s2, test.want)
		}
	}
}

func Test_SortedSliceEqual(t *testing.T) {
	invalid := "invalid"
	assert.Panics(t, func() { SortedSliceEqual(invalid, 1) })
	temp := []int{1, 2, 3}
	assert.Panics(t, func() { SortedSliceEqual(temp, invalid) })

	sortSlice := func(slice interface{}, less func(i, j int) bool) {
		sort.Slice(slice, less)
	}
	intSliceAfterSort := func(slice []int) []int {
		sortSlice(slice, func(i, j int) bool {
			return slice[i] <= slice[j]
		})
		return slice
	}
	stringSliceAfterSort := func(slice []string) []string {
		sortSlice(slice, func(i, j int) bool {
			return slice[i] <= slice[j]
		})
		return slice
	}

	cases := []struct {
		s1   interface{}
		s2   interface{}
		want bool
	}{
		{intSliceAfterSort([]int{}), intSliceAfterSort([]int{}), true},
		{stringSliceAfterSort([]string{}), stringSliceAfterSort([]string{}), true},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{3, 2, 1}), true},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{1, 2, 3}), true},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{}), false},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{1, 2}), false},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{4, 5, 6}), false},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"SliceSetEqual", "test", "for"}), true},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), true},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"test", "for"}), false},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{}), false},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"test", "for", "SliceContain"}), false},
	}

	for _, test := range cases {
		if got := SortedSliceEqual(test.s1, test.s2); got != test.want {
			t.Errorf("SliceSetEqual(%v, %v) = %v", test.s1, test.s2, test.want)
		}
	}
}
