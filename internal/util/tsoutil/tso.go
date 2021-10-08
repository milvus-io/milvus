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

package tsoutil

import (
	"path"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
)

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

// ComposeTS returns a timestamp composed of physical part and logical part
func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << logicalBits) + logical)
}

// ParseTS parses the ts to (physical,logical).
func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

// ParseHybridTs parses the ts to (physical, logical), physical part is of utc-timestamp format.
func ParseHybridTs(ts uint64) (uint64, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	return physical, logical
}

// Mod24H parses the ts to millisecond in one day
func Mod24H(ts uint64) uint64 {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physical = physical % (uint64(24 * 60 * 60 * 1000))
	return (physical << logicalBits) | logical
}

// NewTSOKVBase returns a etcdkv.EtcdKV object
func NewTSOKVBase(etcdEndpoints []string, tsoRoot, subPath string) (*etcdkv.EtcdKV, error) {
	return etcdkv.NewEtcdKV(etcdEndpoints, path.Join(tsoRoot, subPath))
}
