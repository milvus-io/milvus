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

package rootcoord

func diffFileResourceIDs(oldIDs []int64, newIDs []int64) ([]int64, []int64) {
	oldSet := make(map[int64]struct{}, len(oldIDs))
	for _, id := range oldIDs {
		oldSet[id] = struct{}{}
	}
	newSet := make(map[int64]struct{}, len(newIDs))
	for _, id := range newIDs {
		newSet[id] = struct{}{}
	}

	added := make([]int64, 0)
	for _, id := range newIDs {
		if _, ok := oldSet[id]; !ok {
			added = append(added, id)
		}
	}
	removed := make([]int64, 0)
	for _, id := range oldIDs {
		if _, ok := newSet[id]; !ok {
			removed = append(removed, id)
		}
	}
	return added, removed
}
