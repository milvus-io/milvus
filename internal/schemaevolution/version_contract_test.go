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
	"testing"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type versionContractProvider struct {
	sessions map[string]map[string]*sessionutil.Session
	err      error
}

func (p *versionContractProvider) GetSessions(_ context.Context, role string) (map[string]*sessionutil.Session, int64, error) {
	if p.err != nil {
		return nil, 0, p.err
	}
	return p.sessions[role], 1, nil
}

func TestCheckPhase0VersionContract(t *testing.T) {
	newSession := func(id int64, version string) *sessionutil.Session {
		return &sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{ServerID: id, Version: version},
			Version:    semver.MustParse(version),
		}
	}

	t.Run("accepts every registered role at the dev version", func(t *testing.T) {
		sessions := make(map[string]map[string]*sessionutil.Session)
		for _, role := range Phase0VersionContractRoles {
			sessions[role] = map[string]*sessionutil.Session{"node": newSession(1, "3.0.1-dev")}
		}
		require.NoError(t, CheckPhase0VersionContract(context.Background(), &versionContractProvider{sessions: sessions}))
	})

	t.Run("rejects an old node", func(t *testing.T) {
		sessions := map[string]map[string]*sessionutil.Session{
			"querynode": {"node": newSession(7, "3.0.0")},
		}
		err := CheckPhase0VersionContract(context.Background(), &versionContractProvider{sessions: sessions})
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.ErrorContains(t, err, "querynode node 7 reports 3.0.0")
	})

	t.Run("rejects versions at or below the minimum and missing versions", func(t *testing.T) {
		for name, session := range map[string]*sessionutil.Session{
			"old default build": newSession(8, "3.0.0-beta"),
			"missing": {
				SessionRaw: sessionutil.SessionRaw{ServerID: 9},
			},
		} {
			t.Run(name, func(t *testing.T) {
				err := CheckPhase0VersionContract(context.Background(), &versionContractProvider{
					sessions: map[string]map[string]*sessionutil.Session{
						"querynode": {"node": session},
					},
				})
				require.ErrorIs(t, err, merr.ErrServiceNotReady)
			})
		}
	})

	t.Run("accepts development, pre-release, released and later versions", func(t *testing.T) {
		for _, version := range []string{"3.0.1-dev", "3.0.1-rc.1", "3.0.1", "3.0.1+build.1", "3.0.2", "3.1.0"} {
			err := CheckPhase0VersionContract(context.Background(), &versionContractProvider{
				sessions: map[string]map[string]*sessionutil.Session{
					"querynode": {"node": newSession(10, version)},
				},
			})
			require.NoError(t, err)
		}
	})

	t.Run("propagates session lookup failures", func(t *testing.T) {
		expected := errors.New("etcd down")
		err := CheckPhase0VersionContract(context.Background(), &versionContractProvider{err: expected})
		require.ErrorIs(t, err, expected)
	})
}
