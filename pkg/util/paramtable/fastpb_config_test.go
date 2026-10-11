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

package paramtable

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommonEnableFastPBDefaultAndRefresh(t *testing.T) {
	for _, loadYaml := range []bool{false, true} {
		name := "ParamDefault"
		options := []Option{SkipRemote(true), SkipEnv(true)}
		if !loadYaml {
			options = append(options, Files([]string{}))
		} else {
			name = "YamlDefault"
		}
		t.Run(name, func(t *testing.T) {
			base := NewBaseTable(options...)
			cfg := commonConfig{}
			cfg.init(base)
			if loadYaml {
				require.Equal(t, "true", base.Get("common.enableFastPB"), "shipped YAML must contain the enabled default")
			}
			require.Equal(t, "common.enableFastPB", cfg.EnableFastPB.Key)
			require.Equal(t, "true", cfg.EnableFastPB.DefaultValue)
			require.True(t, cfg.EnableFastPB.Export)
			field, ok := reflect.TypeOf(&cfg).Elem().FieldByName("EnableFastPB")
			require.True(t, ok)
			require.Equal(t, "true", field.Tag.Get("refreshable"))
			require.True(t, cfg.EnableFastPB.GetAsBool())
			require.NoError(t, base.Save(cfg.EnableFastPB.Key, "false"))
			require.False(t, cfg.EnableFastPB.GetAsBool())
			require.NoError(t, base.Save(cfg.EnableFastPB.Key, "true"))
			require.True(t, cfg.EnableFastPB.GetAsBool())
		})
	}
}
