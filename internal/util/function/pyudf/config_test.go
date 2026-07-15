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
	params := newPyUDFComponentParam()

	config, err := NewConfig(params)
	require.NoError(t, err)
	assert.False(t, config.Enabled)

	params.FunctionCfg.PyUDFEnabled.SwapTempValue("true")

	config, err = NewConfig(params)
	require.NoError(t, err)
	assert.True(t, config.Enabled)
}

func TestNewConfigInvalid(t *testing.T) {
	tests := []struct {
		name  string
		set   func(*paramtable.ComponentParam)
		match string
	}{
		{
			name:  "nil params",
			set:   nil,
			match: "component parameters are nil",
		},
		{
			name: "malformed enabled",
			set: func(params *paramtable.ComponentParam) {
				params.FunctionCfg.PyUDFEnabled.SwapTempValue("enabled")
			},
			match: "function.pyUDF.enabled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var params *paramtable.ComponentParam
			if test.set != nil {
				params = newPyUDFComponentParam()
				test.set(params)
			}

			_, err := NewConfig(params)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.ErrorContains(t, err, test.match)
		})
	}
}

func newPyUDFComponentParam() *paramtable.ComponentParam {
	params := &paramtable.ComponentParam{}
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	return params
}
