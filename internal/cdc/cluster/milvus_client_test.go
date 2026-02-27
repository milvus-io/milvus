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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestBuildCDCTLSConfig(t *testing.T) {
	paramtable.Init()

	t.Run("no client cert configured returns nil", func(t *testing.T) {
		paramtable.Get().Save("tls.clientPemPath", "")
		paramtable.Get().Save("tls.clientKeyPath", "")

		tlsConfig, err := buildCDCTLSConfig()
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("only caPemPath set returns nil", func(t *testing.T) {
		// Even with caPemPath set, if client certs are not configured, TLS is not activated.
		paramtable.Get().Save("tls.caPemPath", "/some/ca.pem")
		paramtable.Get().Save("tls.clientPemPath", "")
		paramtable.Get().Save("tls.clientKeyPath", "")
		defer paramtable.Get().Save("tls.caPemPath", "")

		tlsConfig, err := buildCDCTLSConfig()
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("partial client cert configured returns nil", func(t *testing.T) {
		// Only clientPemPath set, clientKeyPath empty — should not activate.
		paramtable.Get().Save("tls.clientPemPath", "/some/client.pem")
		paramtable.Get().Save("tls.clientKeyPath", "")
		defer paramtable.Get().Save("tls.clientPemPath", "")

		tlsConfig, err := buildCDCTLSConfig()
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("invalid CA path returns error", func(t *testing.T) {
		paramtable.Get().Save("tls.caPemPath", "/nonexistent/ca.pem")
		paramtable.Get().Save("tls.clientPemPath", "/some/client.pem")
		paramtable.Get().Save("tls.clientKeyPath", "/some/client.key")
		defer func() {
			paramtable.Get().Save("tls.caPemPath", "")
			paramtable.Get().Save("tls.clientPemPath", "")
			paramtable.Get().Save("tls.clientKeyPath", "")
		}()

		_, err := buildCDCTLSConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA cert")
	})
}
