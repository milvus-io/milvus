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

package split

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The gates in front of the planning of a split. Each defaults to refusing.
// dataCoord.shardSplit.enable only turns the trigger on; a split is planned
// only when every gate here lets it through as well.

// readPathReady is the read path gate, see ReadPathReady.
var readPathReady atomic.Bool

// ReadPathReady reports whether the read path of this build serves a split
// source from the DataView: a query of a split collection fans out to the
// source next to its targets, by serving generation, and not to the vchannel
// list of RootCoord, from which the split removes the source.
//
// Until it does, a split removes every row of the source from every search,
// query and count, so the trigger and Manager.Split refuse to plan. It is
// false by default.
//
// Hand-off to layer 6 (view coordinator): call SetReadPathReady(true) once the
// serving generations are wired (desired shards from the DataView, published
// epoch, GetQueryViewLoadInfo returning the list, the epoch and the lineage,
// exact match on the StreamingNode). It is a property of the build, not of a
// deployment: it is not a configuration parameter.
func ReadPathReady() bool {
	return readPathReady.Load()
}

// SetReadPathReady sets the read path gate, see ReadPathReady.
func SetReadPathReady(ready bool) {
	readPathReady.Store(ready)
}

var (
	minimalNodeVersionMu sync.RWMutex
	minimalNodeVersion   semver.Version
)

// MinimalNodeVersion is the first release in which every component that takes
// part in a split understands it: the SplitShard message, its fence and
// genesis (StreamingNode), its ack callback (coordinators), the residue routing
// (proxy) and the lineage reads (QueryNode). A split is planned only if every
// registered session reports at least this version.
//
// No release ships the feature yet, so it is unset (the zero version) and the
// version gate refuses every split. It is to be set, with
// SetMinimalNodeVersion, to the release that ships all six layers. The version
// of this build (common.Version) is not used: the builds of one release before
// and after the feature report the same version.
func MinimalNodeVersion() semver.Version {
	minimalNodeVersionMu.RLock()
	defer minimalNodeVersionMu.RUnlock()
	return minimalNodeVersion
}

// SetMinimalNodeVersion sets the version the version gate requires of every
// node, see MinimalNodeVersion.
func SetMinimalNodeVersion(version semver.Version) {
	minimalNodeVersionMu.Lock()
	defer minimalNodeVersionMu.Unlock()
	minimalNodeVersion = version
}

// NodeVersion is the version one registered session reports.
type NodeVersion struct {
	Role     string
	ServerID int64
	Version  semver.Version
}

// checkGates refuses to plan a split unless the read path serves a split
// source, every node reports a version that knows the feature, and the
// cluster is not part of a replicate topology unless that is allowed
// explicitly.
//
// Why each gate matters once the message is appended: the broadcast task is
// persisted before the copy on the source is appended, and it is retried for
// ever while it holds the exclusive resource key of the collection. A
// StreamingNode that does not know the message refuses it at every attempt,
// so every DDL of the collection waits for good; there is no abort after the
// task is persisted. A secondary that does not know it stops its replication
// stream. And an old DataCoord ignores the split records, the pins and the L0
// suspension of the targets: there is no downgrade after a split.
func (m *Manager) checkGates(ctx context.Context) error {
	if !ReadPathReady() {
		return merr.WrapErrOperationNotSupportedMsg(
			"a shard split is not planned: the read path of this build does not serve a split source yet")
	}
	replicating, err := m.deps.IsReplicating(ctx)
	if err != nil {
		return merr.Wrap(err, "read whether this cluster is part of a replicate topology")
	}
	if replicating && !paramtable.Get().DataCoordCfg.ShardSplitAllowWithReplication.GetAsBool() {
		return merr.WrapErrOperationNotSupportedMsg(
			"a shard split is not planned on a cluster with a replicate topology unless %s is set: the versions of the secondaries cannot be seen from here",
			paramtable.Get().DataCoordCfg.ShardSplitAllowWithReplication.Key)
	}
	minimal := MinimalNodeVersion()
	if minimal.Equals(semver.Version{}) {
		return merr.WrapErrOperationNotSupportedMsg(
			"a shard split is not planned: no release version that ships the feature is declared")
	}
	nodes, err := m.deps.NodeVersions(ctx)
	if err != nil {
		return merr.Wrap(err, "list the versions of the nodes")
	}
	for _, node := range nodes {
		if node.Version.LT(minimal) {
			// A rolling upgrade passes: transient, not an Input error.
			return merr.WrapErrServiceUnavailableMsg(
				"a shard split is not planned: %s %d reports version %s, below %s",
				node.Role, node.ServerID, node.Version.String(), minimal.String())
		}
	}
	return nil
}
