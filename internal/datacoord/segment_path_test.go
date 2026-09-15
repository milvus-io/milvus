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

package datacoord

import "testing"

func TestSegmentBaseMatches(t *testing.T) {
	for _, tc := range []struct {
		base string
		want bool
	}{
		{"insert_log/1/2/3", true},
		{"files/insert_log/1/2/3", true},
		{"/var/lib/milvus/data/files/insert_log/1/2/3", true},
		{"files//insert_log/1/2/3", true},
		{"files/tmp/../insert_log/1/2/3", true},
		{"insert_log/1/2/30", false},
		{"insert_log/1/9/3", false},
		{"insert_log/9/2/3", false},
		{"not_insert_log/1/2/3", false},
		{"insert_log/1/2/3/_stats", false},
		{"", false},
	} {
		t.Run(tc.base, func(t *testing.T) {
			if got := segmentBaseMatches(tc.base, 1, 2, 3); got != tc.want {
				t.Fatalf("segmentBaseMatches(%q) = %v, want %v", tc.base, got, tc.want)
			}
		})
	}
}
