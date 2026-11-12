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

package copysegment

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
)

type restoreSnapshotSource struct{ SnapshotSource }

func (*restoreSnapshotSource) ReadSnapshotData(context.Context, int64, string, bool) (*snapshotstorage.SnapshotData, error) {
	panic("ReadSnapshotData must be patched")
}

func (*restoreSnapshotSource) UnpinSnapshot(context.Context, int64) (int64, string, int, error) {
	panic("UnpinSnapshot must be patched")
}

type restoreCollectionTarget struct{}

func (*restoreCollectionTarget) IsCollectionDropped(context.Context, int64) (bool, error) {
	panic("IsCollectionDropped must be patched")
}

func (*restoreCollectionTarget) CompleteRestoreCollection(context.Context, int64) error {
	panic("CompleteRestoreCollection must be patched")
}

type restoreAllocatorTarget struct{ allocator.Allocator }

func (*restoreAllocatorTarget) AllocN(int64) (int64, int64, error) { panic("AllocN must be patched") }
