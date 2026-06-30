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

package coordinator

import (
	"context"

	qcmeta "github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

// Adapters bind the bump_defence core's small interfaces to the real coordinator
// subsystems. They are intentionally thin: the TargetVersionReader wraps querycoord's
// TargetManager; the DataViewReader is satisfied structurally by datacoord's Server; the
// ProxyGatePusher is satisfied by mixCoordImpl itself (PushGatedFields in mix_coord.go).

// targetVersionReaderAdapter exposes the currentTarget build-time version -- the only
// querycoord state the readiness witness reads. The version is stamped at target
// construction and survives promotion/persistence/recovery, so it is the durable
// evidence that a target built after a given time has passed the promote barrier
// (CheckSegmentDataReady + per-replica delegator serviceability).
type targetVersionReaderAdapter struct {
	tm qcmeta.TargetManagerInterface
}

// NewTargetVersionReader wraps a querycoord TargetManager.
func NewTargetVersionReader(tm qcmeta.TargetManagerInterface) TargetVersionReader {
	return &targetVersionReaderAdapter{tm: tm}
}

func (a *targetVersionReaderAdapter) CurrentTargetVersion(ctx context.Context, collectionID int64) int64 {
	return a.tm.GetCollectionTargetVersion(ctx, collectionID, qcmeta.CurrentTarget)
}
