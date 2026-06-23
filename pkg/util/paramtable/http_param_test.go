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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHTTPConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.HTTPCfg
	assert.Equal(t, cfg.Enabled.GetAsBool(), true)
	assert.Equal(t, cfg.DebugMode.GetAsBool(), false)
	assert.Equal(t, cfg.Port.GetValue(), "")
	assert.Equal(t, cfg.AcceptTypeAllowInt64.GetValue(), "true")
	assert.Equal(t, cfg.EnablePprof.GetAsBool(), true)
	assert.Equal(t, 5*time.Second, cfg.ReadHeaderTimeout.GetAsDurationByParse())
	assert.Equal(t, time.Duration(0), cfg.ReadTimeout.GetAsDurationByParse())
	assert.Equal(t, time.Duration(0), cfg.WriteTimeout.GetAsDurationByParse())
	assert.Equal(t, 300*time.Second, cfg.IdleTimeout.GetAsDurationByParse())
	assert.Equal(t, 16777216, cfg.MaxHeaderBytes.GetAsInt())
	assert.Equal(t, cfg.EnableWebUI.GetAsBool(), true)
}

func TestHTTPConfig_TimeoutOverrides(t *testing.T) {
	params := ComponentParam{}
	base := NewBaseTable(SkipRemote(true))
	params.Init(base)
	cfg := &params.HTTPCfg

	base.Save("proxy.http.readHeaderTimeout", "7s")
	base.Save("proxy.http.readTimeout", "8s")
	base.Save("proxy.http.writeTimeout", "9s")
	base.Save("proxy.http.idleTimeout", "10s")
	base.Save("proxy.http.maxHeaderBytes", "2048")

	assert.Equal(t, 7*time.Second, cfg.ReadHeaderTimeout.GetAsDurationByParse())
	assert.Equal(t, 8*time.Second, cfg.ReadTimeout.GetAsDurationByParse())
	assert.Equal(t, 9*time.Second, cfg.WriteTimeout.GetAsDurationByParse())
	assert.Equal(t, 10*time.Second, cfg.IdleTimeout.GetAsDurationByParse())
	assert.Equal(t, 2048, cfg.MaxHeaderBytes.GetAsInt())
}
