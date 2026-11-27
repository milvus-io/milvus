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

//go:build milvus_fips

package fips

import (
	"os"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequireGoFIPS(t *testing.T) {
	require.ErrorContains(t, requireGoFIPS(nil, true), "GOFIPS140")
	require.ErrorContains(t, requireGoFIPS(&debug.BuildInfo{}, true), "GOFIPS140")
	for _, snapshot := range []string{"latest", "v1.26.0", "v1.0.0", "v1.0.0-other"} {
		t.Run(snapshot, func(t *testing.T) {
			info := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "GOFIPS140", Value: snapshot}}}
			require.ErrorContains(t, requireGoFIPS(info, true), "GOFIPS140")
		})
	}

	info := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "GOFIPS140", Value: certifiedGoSnapshot}}}
	require.ErrorContains(t, requireGoFIPS(info, false), "mode is off")
	require.NoError(t, requireGoFIPS(info, true))
}

// Runtime images install the provider and its integrity configuration together.
func TestOpenSSLFIPSWithInstalledConfig(t *testing.T) {
	_, err := os.Stat(opensslConfigPath)
	if os.IsNotExist(err) {
		t.Skip("Milvus OpenSSL configuration is not installed")
	}
	require.NoError(t, err)
	require.True(t, MaybeEnableOpenSSLFIPS())
	require.Contains(t, Status(), "OpenSSL FIPS provider 3.1.2")
}
