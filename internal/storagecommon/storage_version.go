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

package storagecommon

import "github.com/milvus-io/milvus/pkg/v2/util/merr"

const (
	StorageV1 int64 = 0
	// StorageV1LegacyAlias is the equivalent V1 version in persisted metadata.
	StorageV1LegacyAlias int64 = 1
	StorageV2            int64 = 2
)

// ValidateSegmentStorageVersion checks whether this node can recover the segment.
// Both 0 and 1 occur in legacy V1 metadata.
func ValidateSegmentStorageVersion(segmentID, version int64) error {
	switch version {
	case StorageV1, StorageV1LegacyAlias, StorageV2:
		return nil
	default:
		return merr.Wrapf(merr.ErrServiceUnimplemented, "segment %d has unsupported storage version %d", segmentID, version)
	}
}
