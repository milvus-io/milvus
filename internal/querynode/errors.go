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

package querynode

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
)

var (
	// ErrShardNotAvailable shard not available error base.
	ErrShardNotAvailable = errors.New("ShardNotAvailable")
	// ErrTsLagTooLarge serviceable and guarantee lag too large.
	ErrTsLagTooLarge = errors.New("Timestamp lag too large")
	// ErrInsufficientMemory returns insufficient memory error.
	ErrInsufficientMemory = errors.New("InsufficientMemoryToLoad")
)

// WrapErrShardNotAvailable wraps ErrShardNotAvailable with replica id and channel name.
func WrapErrShardNotAvailable(replicaID int64, shard string) error {
	return fmt.Errorf("%w(replica=%d, shard=%s)", ErrShardNotAvailable, replicaID, shard)
}

// WrapErrTsLagTooLarge wraps ErrTsLagTooLarge with lag and max value.
func WrapErrTsLagTooLarge(duration time.Duration, maxLag time.Duration) error {
	return fmt.Errorf("%w lag(%s) max(%s)", ErrTsLagTooLarge, duration, maxLag)
}

// msgQueryNodeIsUnhealthy is the error msg of unhealthy query node
func msgQueryNodeIsUnhealthy(nodeID UniqueID) string {
	return fmt.Sprintf("query node %d is not ready", nodeID)
}

// errQueryNodeIsUnhealthy is the error of query node is unhealthy
func errQueryNodeIsUnhealthy(nodeID UniqueID) error {
	return errors.New(msgQueryNodeIsUnhealthy(nodeID))
}
