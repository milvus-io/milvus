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

package externalspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExternalSpec_Empty(t *testing.T) {
	spec, err := ParseExternalSpec("")
	require.NoError(t, err)
	assert.Equal(t, "parquet", spec.Format)
}

func TestParseExternalSpec_InvalidJSON(t *testing.T) {
	_, err := ParseExternalSpec("{bad json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid external spec JSON")
}

func TestParseExternalSpec_DefaultFormat(t *testing.T) {
	spec, err := ParseExternalSpec(`{"columns":["a","b"]}`)
	require.NoError(t, err)
	assert.Equal(t, "parquet", spec.Format)
	assert.Equal(t, []string{"a", "b"}, spec.Columns)
}

func TestParseExternalSpec_SupportedFormats(t *testing.T) {
	for _, fmt := range []string{"parquet", "lance-table", "vortex"} {
		spec, err := ParseExternalSpec(`{"format":"` + fmt + `"}`)
		require.NoError(t, err, "format %s should be supported", fmt)
		assert.Equal(t, fmt, spec.Format)
	}
}

func TestParseExternalSpec_UnsupportedFormat(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"csv"}`)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported format")
	assert.Contains(t, err.Error(), "lance-table")
	assert.Contains(t, err.Error(), "parquet")
	assert.Contains(t, err.Error(), "vortex")
}

func TestParseExternalSpec_ValidExtfs(t *testing.T) {
	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"region":"us-west-2","use_ssl":"true"}}`)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", spec.Extfs["region"])
	assert.Equal(t, "true", spec.Extfs["use_ssl"])
}

func TestParseExternalSpec_NotAllowedExtfs(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"shell_command":"rm -rf /"}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed")
}

func TestParseExternalSpec_BooleanExtfsValidation(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"yes"}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be \"true\" or \"false\"")

	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"true"}}`)
	require.NoError(t, err)
	assert.Equal(t, "true", spec.Extfs["use_iam"])
}
