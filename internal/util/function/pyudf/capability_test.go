// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestValidateConfigCapability(t *testing.T) {
	unavailable := BuildCapability{Reason: "runtime was not compiled"}
	available := BuildCapability{Available: true}

	assert.NoError(t, ValidateConfigCapability(Config{Enabled: false}, unavailable))
	assert.NoError(t, ValidateConfigCapability(Config{Enabled: true}, available))

	err := ValidateConfigCapability(Config{Enabled: true}, unavailable)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "function.pyUDF.enabled is true")
	assert.ErrorContains(t, err, "runtime was not compiled")

	err = ValidateConfigCapability(Config{Enabled: true}, BuildCapability{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "embedded PyUDF runtime is unavailable")
}
