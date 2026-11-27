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

package common

import "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

// GetAddedLoadFieldIDs returns only newly introduced, default-preloaded user
// fields. Existing omissions remain intentional; this is not a metadata repair.
// Struct containers are not loadable fields, but their children are.
func GetAddedLoadFieldIDs(previous, target *schemapb.CollectionSchema) []int64 {
	visit := func(schema *schemapb.CollectionSchema, fn func(*schemapb.FieldSchema)) {
		for _, field := range schema.GetFields() {
			fn(field)
		}
		for _, parent := range schema.GetStructArrayFields() {
			for _, field := range parent.GetFields() {
				fn(field)
			}
		}
	}
	seen := make(map[int64]struct{})
	visit(previous, func(field *schemapb.FieldSchema) { seen[field.GetFieldID()] = struct{}{} })
	var added []int64
	visit(target, func(field *schemapb.FieldSchema) {
		id := field.GetFieldID()
		if id < StartOfUserFieldID {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		preload, err := ShouldFieldBeLoaded(field.GetTypeParams())
		// Match the public load API: malformed skip_load defaults to loading.
		if err != nil || preload {
			added = append(added, id)
		}
	})
	return added
}
