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

package fileresource

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGetStandaloneMode(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	oldRole := paramtable.GetRole()
	defer paramtable.SetRole(oldRole)

	tests := []struct {
		name string
		qn   string
		dn   string
		pn   string
		want Mode
	}{
		{name: "all close", qn: "close", dn: "close", pn: "close", want: CloseMode},
		{name: "all close ignores case and spaces", qn: " CLOSE ", dn: "Close", pn: "close", want: CloseMode},
		{name: "all sync", qn: "sync", dn: "sync", pn: "sync", want: SyncMode},
		{name: "one sync", qn: "close", dn: "close", pn: "sync", want: SyncMode},
		{name: "data node ref", qn: "close", dn: "ref", pn: "close", want: SyncMode},
		{name: "mixed modes", qn: "sync", dn: "ref", pn: "close", want: SyncMode},
		{name: "invalid does not disable", qn: "invalid", dn: "close", pn: "close", want: SyncMode},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params.CommonCfg.QNFileResourceMode.SwapTempValue(test.qn)
			params.CommonCfg.DNFileResourceMode.SwapTempValue(test.dn)
			params.CommonCfg.PNFileResourceMode.SwapTempValue(test.pn)
			defer params.CommonCfg.QNFileResourceMode.SwapTempValue("sync")
			defer params.CommonCfg.DNFileResourceMode.SwapTempValue("sync")
			defer params.CommonCfg.PNFileResourceMode.SwapTempValue("sync")

			require.Equal(t, test.want, GetStandaloneMode())
		})
	}
}

func TestResolveLocalMode(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	oldRole := paramtable.GetRole()
	defer paramtable.SetRole(oldRole)
	defer params.CommonCfg.QNFileResourceMode.SwapTempValue("sync")
	defer params.CommonCfg.DNFileResourceMode.SwapTempValue("sync")
	defer params.CommonCfg.PNFileResourceMode.SwapTempValue("sync")

	tests := []struct {
		name     string
		role     string
		roles    LocalRoles
		qn       string
		dn       string
		pn       string
		expected Mode
	}{
		{name: "query node sync", role: typeutil.MixtureRole, roles: LocalRoles{QueryNode: true}, qn: "sync", dn: "ref", pn: "close", expected: SyncMode},
		{name: "query node close", role: typeutil.MixtureRole, roles: LocalRoles{QueryNode: true}, qn: "close", dn: "sync", pn: "sync", expected: CloseMode},
		{name: "data node ref", role: typeutil.MixtureRole, roles: LocalRoles{DataNode: true}, qn: "sync", dn: "ref", pn: "sync", expected: RefMode},
		{name: "streaming node follows query node", role: typeutil.MixtureRole, roles: LocalRoles{StreamingNode: true}, qn: "sync", dn: "close", pn: "close", expected: SyncMode},
		{name: "sync overrides ref", role: typeutil.MixtureRole, roles: LocalRoles{QueryNode: true, DataNode: true}, qn: "sync", dn: "ref", pn: "close", expected: SyncMode},
		{name: "ref overrides close", role: typeutil.MixtureRole, roles: LocalRoles{QueryNode: true, DataNode: true}, qn: "close", dn: "ref", pn: "close", expected: RefMode},
		{name: "proxy sync overrides data node ref", role: typeutil.MixtureRole, roles: LocalRoles{DataNode: true, Proxy: true}, qn: "close", dn: "ref", pn: "sync", expected: SyncMode},
		{name: "disabled roles ignored", role: typeutil.MixtureRole, roles: LocalRoles{Proxy: true}, qn: "sync", dn: "ref", pn: "close", expected: CloseMode},
		{name: "standalone all close", role: typeutil.StandaloneRole, roles: LocalRoles{QueryNode: true, DataNode: true, Proxy: true}, qn: "close", dn: "close", pn: "close", expected: CloseMode},
		{name: "standalone ref promotes to sync", role: typeutil.StandaloneRole, roles: LocalRoles{QueryNode: true, DataNode: true, Proxy: true}, qn: "close", dn: "ref", pn: "close", expected: SyncMode},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			paramtable.SetRole(test.role)
			params.CommonCfg.QNFileResourceMode.SwapTempValue(test.qn)
			params.CommonCfg.DNFileResourceMode.SwapTempValue(test.dn)
			params.CommonCfg.PNFileResourceMode.SwapTempValue(test.pn)
			require.Equal(t, test.expected, ResolveLocalMode(test.roles))
		})
	}
}

func TestLocalMode(t *testing.T) {
	ResetLocalModeForTest()
	defer ResetLocalModeForTest()

	require.Equal(t, RefMode, GetLocalMode(RefMode))
	SetLocalMode(SyncMode)
	require.Equal(t, SyncMode, GetLocalMode(CloseMode))
}

func TestGetRoleModes(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	oldRole := paramtable.GetRole()
	defer paramtable.SetRole(oldRole)
	defer params.CommonCfg.QNFileResourceMode.SwapTempValue("sync")
	defer params.CommonCfg.DNFileResourceMode.SwapTempValue("sync")
	defer params.CommonCfg.PNFileResourceMode.SwapTempValue("sync")

	params.CommonCfg.QNFileResourceMode.SwapTempValue("sync")
	params.CommonCfg.DNFileResourceMode.SwapTempValue("ref")
	params.CommonCfg.PNFileResourceMode.SwapTempValue("close")

	t.Run("standalone uses one effective mode", func(t *testing.T) {
		paramtable.SetRole(typeutil.StandaloneRole)
		require.Equal(t, SyncMode, GetQueryNodeMode())
		require.Equal(t, SyncMode, GetDataNodeMode())
		require.Equal(t, SyncMode, GetProxyMode())
	})

	t.Run("cluster preserves role modes", func(t *testing.T) {
		paramtable.SetRole(typeutil.QueryNodeRole)
		require.Equal(t, SyncMode, GetQueryNodeMode())
		require.Equal(t, RefMode, GetDataNodeMode())
		require.Equal(t, CloseMode, GetProxyMode())
	})

	t.Run("cluster proxy ref is normalized to close", func(t *testing.T) {
		paramtable.SetRole(typeutil.ProxyRole)
		params.CommonCfg.PNFileResourceMode.SwapTempValue("ref")
		require.Equal(t, CloseMode, GetProxyMode())
	})
}
