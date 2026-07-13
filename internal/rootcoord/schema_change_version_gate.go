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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// minSchemaChangeMajorVersion is the lowest component major version that understands the
// online schema-change protocol end to end: the streamingnode "accept any write at or behind
// current, reconcile the gap" write gate, the delegator read version gate, and the proxy-last
// synchronous DDL handshake all landed in 3.0. A component below this -- typically a 2.x node
// still alive during a rolling upgrade -- has none of them, so a schema-changing write could
// reach it unversioned and be interpreted against the wrong schema. Schema DDL is therefore
// refused until every live component is >= 3.0.
const minSchemaChangeMajorVersion = 3

// schemaChangeGatedRoles are the component roles whose live sessions are inspected before a
// schema-mutating DDL is admitted. It is the full set of nodes and coordinators that take part
// in the write / read / backfill path; a single pre-3.0 member among any of them is enough to
// refuse the change. Roles absent from a given deployment simply return no sessions.
var schemaChangeGatedRoles = []string{
	typeutil.RootCoordRole,
	typeutil.ProxyRole,
	typeutil.QueryCoordRole,
	typeutil.QueryNodeRole,
	typeutil.DataCoordRole,
	typeutil.DataNodeRole,
	typeutil.IndexCoordRole,
	typeutil.IndexNodeRole,
	typeutil.StreamingCoordRole,
	typeutil.StreamingNodeRole,
	typeutil.MixCoordRole,
}

// checkClusterVersionForSchemaChange refuses a schema-mutating DDL while any live component is
// below minSchemaChangeMajorVersion. It is a rolling-upgrade guard: once every component is on
// 3.0+ it always passes, and it can be turned off at runtime via
// rootCoord.enableSchemaChangeVersionGate for an operator who must force a change through.
//
// It fails CLOSED. If the session registry cannot be read, or a session carries no parseable
// version (a zero major), the DDL is refused rather than risked -- an unverifiable cluster is
// treated as potentially mixed-version.
//
// The gate is skipped when the session lister is not wired (c.sessionLister == nil). That is
// only the case for unit tests that construct Core directly instead of going through
// SetSession; a real deployment always has a registry to inspect.
func (c *Core) checkClusterVersionForSchemaChange(ctx context.Context) error {
	if !Params.RootCoordCfg.EnableSchemaChangeVersionGate.GetAsBool() {
		return nil
	}
	if c.sessionLister == nil {
		return nil
	}
	for _, role := range schemaChangeGatedRoles {
		sessions, err := c.sessionLister(ctx, role)
		if err != nil {
			// Fail closed: if we cannot confirm every component is new enough, do not admit the
			// change. This is retriable -- the operator can try again once the registry is reachable.
			return merr.WrapErrServiceUnavailableMsg(
				"cannot verify cluster component versions before schema change; try again later (listing %s sessions failed: %v)",
				role, err)
		}
		if err := checkSessionsMeetSchemaChangeVersion(role, sessions); err != nil {
			mlog.Warn(ctx, "online schema change refused: cluster is not fully on a compatible version",
				mlog.Err(err))
			return err
		}
	}
	return nil
}

// checkSessionsMeetSchemaChangeVersion is the pure decision underneath the gate: reject if any
// of the given sessions is below the minimum major version. Split out from the etcd-backed
// listing so it can be unit-tested without a registry. A zero-value version (major 0) fails the
// check, keeping the gate closed for a session that registered without a parseable version.
func checkSessionsMeetSchemaChangeVersion(role string, sessions map[string]*sessionutil.Session) error {
	for _, s := range sessions {
		if s.Version.Major < minSchemaChangeMajorVersion {
			return merr.WrapErrServiceUnavailableMsg(
				"online schema change is refused while the cluster still contains a pre-%d.0 component: "+
					"%s (serverID=%d, addr=%s) is at version %q; finish upgrading every component to %d.0+ first, "+
					"or set rootCoord.enableSchemaChangeVersionGate=false to override",
				minSchemaChangeMajorVersion, role, s.ServerID, s.Address, s.Version.String(), minSchemaChangeMajorVersion)
		}
	}
	return nil
}
