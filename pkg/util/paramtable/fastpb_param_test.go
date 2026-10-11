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
	bt := NewBaseTable(SkipRemote(true), SkipEnv(true))
	t.Cleanup(bt.mgr.Close)
	params := &commonConfig{}
	params.init(bt)
	item := &params.EnableFastPB
	require.Equal(t, "common.enableFastPB", item.Key)
	require.Equal(t, "true", item.DefaultValue)
	require.True(t, item.Export)
	field, ok := reflect.TypeOf(params).Elem().FieldByName("EnableFastPB")
	require.True(t, ok)
	require.Equal(t, "true", field.Tag.Get("refreshable"))
	require.True(t, item.GetAsBool())
	require.NoError(t, bt.Save(item.Key, "false"))
	require.False(t, item.GetAsBool())
	require.NoError(t, bt.Save(item.Key, "true"))
	require.True(t, item.GetAsBool())
	bt.Reset(item.Key)
	require.True(t, item.GetAsBool())
}
