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

package segcore

// StorageCost aggregates bytes scanned during a segment operation. Populated by
// the C++ side after operations that touch cold/remote storage, and propagated
// to the client via SearchResults/QueryResults for observability.
type StorageCost struct {
	ScannedRemoteBytes int64
	ScannedTotalBytes  int64
	// Valid is true only when every contributing storage-bearing segment
	// accounted its bytes. Its zero value is deliberately false so results from
	// older QueryNodes are treated as incomplete during rolling upgrades.
	Valid bool
}

// Add merges another independently measured contribution. Callers must start
// the accumulator with Valid=true so validity is AND-reduced; the zero value
// intentionally remains invalid for backward compatibility with old nodes.
func (c *StorageCost) Add(other StorageCost) {
	c.ScannedRemoteBytes += other.ScannedRemoteBytes
	c.ScannedTotalBytes += other.ScannedTotalBytes
	c.Valid = c.Valid && other.Valid
}
