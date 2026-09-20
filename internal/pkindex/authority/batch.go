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

package authority

// Batch collects changes in memory. Nothing reaches the engine before Authority.Write.
// Changes are applied in the order of the Put and Delete calls that collected them.
type Batch struct {
	muts []Mutation
}

// Put maps pk to e.
func (b *Batch) Put(pk PK, e Entry) {
	b.muts = append(b.muts, Mutation{Key: pk.Encode(), Value: e.encode()})
}

// Delete makes pk absent.
func (b *Batch) Delete(pk PK) {
	b.muts = append(b.muts, Mutation{Key: pk.Encode(), Delete: true})
}

// Len returns the number of collected changes.
func (b *Batch) Len() int {
	return len(b.muts)
}
