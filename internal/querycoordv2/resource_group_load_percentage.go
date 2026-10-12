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

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetLoadPercentageByResourceGroup reports how loaded collectionID is,
// restricted to the replica(s) that live in resource group rgName; an empty
// rgName means every replica of the collection. See
// utils.LoadPercentageByResourceGroup for the semantics of the returned
// percentage, including the deliberate difference between -1 ("no replica in
// this resource group") and 0 ("a replica is there but carries nothing yet"),
// and the two states that only the error distinguishes: -1 with
// ErrServiceNotReady means the coordinator's read stores are not wired up yet,
// so nothing is known about any resource group, and -1 with
// ErrCollectionNotLoaded means the load failed terminally, with the recorded
// cause in the message. The two must not be conflated -- only the first is
// worth retrying. Like the RPC surfaces on Server, this method gates on
// merr.CheckHealthy(s.State()) first, reporting a coordinator that is not
// serving yet with that same ErrServiceNotReady.
//
// The computation itself lives in the utils package rather than here because
// CollectionObserver needs the same figure and cannot import this package.
// This method exists so external callers, which reach querycoord through
// Server, keep a stable entry point.
func (s *Server) GetLoadPercentageByResourceGroup(ctx context.Context, collectionID int64, rgName string) (int32, error) {
	// The health gate, not the nil checks downstream, is what actually makes
	// this safe to call on a Server that is still coming up. s.meta/s.dist/
	// s.targetMgr are plain fields with no synchronization, so a nil check on
	// them carries no happens-before edge; the atomic status store that
	// Server.Start performs after wiring everything is the edge, and
	// CheckHealthy is how the rest of Server's surface consumes it. Not
	// healthy answers ErrServiceNotReady, which is the same code
	// LoadPercentageByResourceGroup uses for the same condition.
	if err := merr.CheckHealthy(s.State()); err != nil {
		return -1, err
	}
	return utils.LoadPercentageByResourceGroup(ctx, s.meta, s.targetMgr, s.dist, collectionID, rgName)
}
