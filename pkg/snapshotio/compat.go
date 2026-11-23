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

package snapshotio

import "context"

const (
	StorageV2 int64 = 2
	StorageV3 int64 = 3
)

// ChunkManager is the existing key-based Snapshot IO contract. CopySnapshot adapts
// its bucket-aware ObjectReader/ObjectStore dependencies to this interface internally.
type ChunkManager interface {
	RootPath() string
	Read(context.Context, string) ([]byte, error)
	Write(context.Context, string, []byte) error
	Remove(context.Context, string) error
	MultiRemove(context.Context, []string) error
}
