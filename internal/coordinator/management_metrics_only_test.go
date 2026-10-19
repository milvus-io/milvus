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

package coordinator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestHandleAlterConfigRejectsManagementMetricsOnly(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.CommonCfg.ManagementMetricsOnly.Key
	previous := params.CommonCfg.ManagementMetricsOnly.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, previous)) })
	coord := &mixCoordImpl{}
	for _, mode := range []string{"false", "true"} {
		require.NoError(t, params.Save(key, mode))
		for _, alias := range []string{
			key, "COMMON.SECURITY.MANAGEMENTMETRICSONLY", "common_security_managementMetricsOnly",
			"common/security/managementMetricsOnly", "commonsecuritymanagementmetricsonly",
			"co_mmon/secu.rity/man_agementMetricsOnly",
		} {
			for _, value := range []interface{}{"true", "false", "", nil} {
				entry := map[string]interface{}{"key": alias, "value": value}
				for _, payload := range []interface{}{entry, map[string]interface{}{"configs": []interface{}{
					map[string]interface{}{"key": "test.metrics.only.must.not.be.written", "value": "canary"}, entry,
				}}} {
					body, err := json.Marshal(payload)
					require.NoError(t, err)
					w := httptest.NewRecorder()
					coord.HandleAlterConfig(w, httptest.NewRequest(http.MethodPost, "/management/config/alter", bytes.NewReader(body)))
					assert.Equal(t, http.StatusBadRequest, w.Code, "%s", body)
					assert.Contains(t, w.Body.String(), "managementMetricsOnly cannot be modified")
					assert.Equal(t, mode, params.CommonCfg.ManagementMetricsOnly.GetValue())
					_, _, err = paramtable.GetBaseTable().Manager().GetConfig("test.metrics.only.must.not.be.written")
					assert.Error(t, err, "reject the entire batch before writing any entry")
				}
			}
		}
	}
}
