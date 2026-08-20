// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemaevolution

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Phase0MinimumNodeVersion is the lowest version that advertises the
// schema-install gate and schema/barrier receiver fence. Nodes must report a
// version strictly greater than this constant: the released 3.0.0 predates the
// Phase 0 protocol, while pre-release development builds of this branch (for
// example 3.0.1-dev) already contain it. It is deliberately independent of
// pkg/common.Version while the repository is being developed against a
// pre-release version.
var Phase0MinimumNodeVersion = semver.MustParse("3.0.0")

// Phase0VersionContractRoles contains every service role whose session may
// participate in schema DDL or receive schema-dependent state. MixCoord is the
// only supported coordinator deployment for Phase 0; the legacy coordinator
// aliases are included so a stale independently deployed coordinator fails
// closed instead of being silently ignored.
var Phase0VersionContractRoles = []string{
	typeutil.StandaloneRole,
	typeutil.EmbeddedRole,
	typeutil.MixCoordRole,
	typeutil.RootCoordRole,
	typeutil.QueryCoordRole,
	typeutil.DataCoordRole,
	typeutil.IndexCoordRole,
	typeutil.ProxyRole,
	typeutil.QueryNodeRole,
	typeutil.DataNodeRole,
	typeutil.IndexNodeRole,
	typeutil.StreamingNodeRole,
	typeutil.StreamingCoordRole,
}

// SessionProvider is the part of sessionutil.Session used by the version
// contract. Keeping this small makes the check straightforward to test and
// avoids coupling the gate manager to a concrete etcd implementation.
type SessionProvider interface {
	GetSessions(ctx context.Context, prefix string) (map[string]*sessionutil.Session, int64, error)
}

// CheckPhase0VersionContract verifies that every currently registered service
// understands the Phase 0 protocol. A missing role is valid (for example an
// unloaded cluster may have no QueryNode), while a present old or malformed
// session blocks schema-changing DDL with a retriable system error.
func CheckPhase0VersionContract(ctx context.Context, provider SessionProvider) error {
	if provider == nil {
		return merr.WrapErrServiceNotReadyMsg("schema install version contract provider is not initialized")
	}

	for _, role := range Phase0VersionContractRoles {
		sessions, _, err := provider.GetSessions(ctx, role)
		if err != nil {
			return merr.Wrapf(err, "failed to inspect %s sessions for schema install version contract", role)
		}
		for key, sess := range sessions {
			if sess == nil {
				return merr.WrapErrServiceNotReadyMsg(
					"schema install requires all nodes to be strictly newer than %s; %s session %s is empty",
					Phase0MinimumNodeVersion,
					role,
					key)
			}
			version := sess.Version
			if version.Equals(semver.Version{}) && sess.SessionRaw.Version != "" {
				parsed, parseErr := semver.Parse(sess.SessionRaw.Version)
				if parseErr != nil {
					return merr.WrapErrServiceNotReadyMsg(
						"schema install requires all nodes to be strictly newer than %s; %s node %d reports malformed version %q",
						Phase0MinimumNodeVersion,
						role,
						sess.ServerID,
						sess.SessionRaw.Version)
				}
				version = parsed
			}
			// Every node must be strictly newer than the released 3.0.0 that
			// predates the Phase 0 protocol. Development builds (3.0.1-dev) and
			// later releases qualify; build metadata does not affect comparison.
			if !version.GT(Phase0MinimumNodeVersion) {
				return merr.WrapErrServiceNotReadyMsg(
					"schema install requires all nodes to be strictly newer than %s; %s node %d reports %s",
					Phase0MinimumNodeVersion,
					role,
					sess.ServerID,
					sessionVersionString(sess))
			}
		}
	}
	return nil
}

func sessionVersionString(sess *sessionutil.Session) string {
	if sess == nil {
		return "<nil>"
	}
	if sess.Version.Equals(semver.Version{}) && sess.SessionRaw.Version != "" {
		return sess.SessionRaw.Version
	}
	return fmt.Sprint(sess.Version)
}
