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

package typeutil

// UniqueSet is set type, which contains only UniqueIDs,
// the underlying type is map[UniqueID]struct{}.
// Create a UniqueSet instance with make(UniqueSet) like creating a map instance.
type UniqueSet map[UniqueID]struct{}

// Insert elements into the set,
// do nothing if the id existed
func (set UniqueSet) Insert(ids ...UniqueID) {
	for i := range ids {
		set[ids[i]] = struct{}{}
	}
}

// Check whether the elements exist
func (set UniqueSet) Contain(ids ...UniqueID) bool {
	for i := range ids {
		_, ok := set[ids[i]]
		if !ok {
			return false
		}
	}

	return true
}

// Remove elements from the set,
// do nothing if set is nil or id not exists
func (set UniqueSet) Remove(ids ...UniqueID) {
	for i := range ids {
		delete(set, ids[i])
	}
}

// Get all elements in the set
func (set UniqueSet) Collect() []UniqueID {
	ids := make([]UniqueID, 0, len(set))

	for id := range set {
		ids = append(ids, id)
	}

	return ids
}
