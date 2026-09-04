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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestNewConfig(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	oldValue := item.SwapTempValue("false")
	t.Cleanup(func() { item.SwapTempValue(oldValue) })

	config := NewConfig()
	assert.False(t, config.Enabled)

	item.SwapTempValue("true")
	config = NewConfig()
	assert.True(t, config.Enabled)
}

func TestNewConfigInvalidBoolDefaultsToFalse(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	oldValue := item.SwapTempValue("enabled")
	t.Cleanup(func() { item.SwapTempValue(oldValue) })

	config := NewConfig()
	assert.False(t, config.Enabled)
}

func TestCheckEnabled(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	oldValue := item.SwapTempValue("false")
	t.Cleanup(func() { item.SwapTempValue(oldValue) })

	err := CheckEnabled()
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "function.pyUDF.enabled is false")

	item.SwapTempValue("true")
	assert.NoError(t, CheckEnabled())
}
