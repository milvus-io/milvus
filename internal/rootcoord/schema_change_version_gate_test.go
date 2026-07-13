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
	"testing"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func mkSession(id int64, ver string) *sessionutil.Session {
	return &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{ServerID: id, Address: "127.0.0.1:1"},
		Version:    semver.MustParse(ver),
	}
}

func TestCheckSessionsMeetSchemaChangeVersion(t *testing.T) {
	// no sessions of this role -> nothing to reject
	assert.NoError(t, checkSessionsMeetSchemaChangeVersion(typeutil.ProxyRole, nil))

	// every session on 3.x, including the 3.0.0-beta the current build reports (a pre-release
	// still has major 3, so it must pass) -> allowed
	ok := map[string]*sessionutil.Session{
		"a": mkSession(1, "3.0.0"),
		"b": mkSession(2, "3.0.0-beta"),
		"c": mkSession(3, "3.1.4"),
	}
	assert.NoError(t, checkSessionsMeetSchemaChangeVersion(typeutil.ProxyRole, ok))

	// a single pre-3.0 member -> refused
	mixed := map[string]*sessionutil.Session{
		"a": mkSession(1, "3.0.0"),
		"b": mkSession(2, "2.5.9"),
	}
	assert.ErrorIs(t, checkSessionsMeetSchemaChangeVersion(typeutil.QueryNodeRole, mixed), merr.ErrServiceUnavailable)

	// a session with no parseable version (zero value, major 0) -> fail closed
	zero := map[string]*sessionutil.Session{"a": {SessionRaw: sessionutil.SessionRaw{ServerID: 7}}}
	assert.ErrorIs(t, checkSessionsMeetSchemaChangeVersion(typeutil.DataNodeRole, zero), merr.ErrServiceUnavailable)
}

func TestCheckClusterVersionForSchemaChange(t *testing.T) {
	key := Params.RootCoordCfg.EnableSchemaChangeVersionGate.Key
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	ctx := context.Background()

	all3x := func(ctx context.Context, role string) (map[string]*sessionutil.Session, error) {
		return map[string]*sessionutil.Session{role: mkSession(1, "3.0.0")}, nil
	}
	proxyOld := func(ctx context.Context, role string) (map[string]*sessionutil.Session, error) {
		if role == typeutil.ProxyRole {
			return map[string]*sessionutil.Session{"p": mkSession(9, "2.5.0")}, nil
		}
		return map[string]*sessionutil.Session{role: mkSession(1, "3.0.0")}, nil
	}
	listErr := func(ctx context.Context, role string) (map[string]*sessionutil.Session, error) {
		return nil, errors.New("etcd unreachable")
	}

	// gate on, whole cluster on 3.x -> admitted
	paramtable.Get().Save(key, "true")
	assert.NoError(t, (&Core{sessionLister: all3x}).checkClusterVersionForSchemaChange(ctx))

	// gate on, a lingering 2.x proxy -> refused
	assert.ErrorIs(t, (&Core{sessionLister: proxyOld}).checkClusterVersionForSchemaChange(ctx), merr.ErrServiceUnavailable)

	// gate on, cannot list sessions -> fail closed (refused)
	assert.ErrorIs(t, (&Core{sessionLister: listErr}).checkClusterVersionForSchemaChange(ctx), merr.ErrServiceUnavailable)

	// gate on but no lister wired (a Core built directly in a unit test) -> skipped, no panic
	assert.NoError(t, (&Core{}).checkClusterVersionForSchemaChange(ctx))

	// operator override: gate disabled lets a 2.x proxy through
	paramtable.Get().Save(key, "false")
	assert.NoError(t, (&Core{sessionLister: proxyOld}).checkClusterVersionForSchemaChange(ctx))
}
