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

	t.Run("no config for cluster returns nil", func(t *testing.T) {
		tlsConfig, err := buildCDCTLSConfig("unknown-cluster")
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("only caPemPath set returns nil", func(t *testing.T) {
		paramtable.Get().Save("tls.clusters.test-cluster.caPemPath", "/some/ca.pem")
		defer paramtable.Get().Save("tls.clusters.test-cluster.caPemPath", "")

		tlsConfig, err := buildCDCTLSConfig("test-cluster")
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("partial client cert configured returns nil", func(t *testing.T) {
		paramtable.Get().Save("tls.clusters.test-cluster.clientPemPath", "/some/client.pem")
		defer paramtable.Get().Save("tls.clusters.test-cluster.clientPemPath", "")

		tlsConfig, err := buildCDCTLSConfig("test-cluster")
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("invalid CA path returns error", func(t *testing.T) {
		paramtable.Get().Save("tls.clusters.test-cluster.caPemPath", "/nonexistent/ca.pem")
		paramtable.Get().Save("tls.clusters.test-cluster.clientPemPath", "/some/client.pem")
		paramtable.Get().Save("tls.clusters.test-cluster.clientKeyPath", "/some/client.key")
		defer func() {
			paramtable.Get().Save("tls.clusters.test-cluster.caPemPath", "")
			paramtable.Get().Save("tls.clusters.test-cluster.clientPemPath", "")
			paramtable.Get().Save("tls.clusters.test-cluster.clientKeyPath", "")
		}()

		_, err := buildCDCTLSConfig("test-cluster")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA cert")
	})

	t.Run("different clusters use different configs", func(t *testing.T) {
		paramtable.Get().Save("tls.clusters.cluster-a.clientPemPath", "/certs/a/client.pem")
		paramtable.Get().Save("tls.clusters.cluster-b.clientPemPath", "/certs/b/client.pem")
		defer func() {
			paramtable.Get().Save("tls.clusters.cluster-a.clientPemPath", "")
			paramtable.Get().Save("tls.clusters.cluster-b.clientPemPath", "")
		}()

		// cluster-a has only clientPemPath (no key) → nil
		tlsConfig, err := buildCDCTLSConfig("cluster-a")
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)

		// cluster-b also has only clientPemPath (no key) → nil
		tlsConfig, err = buildCDCTLSConfig("cluster-b")
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)

		// cluster-c has no config at all → nil
		tlsConfig, err = buildCDCTLSConfig("cluster-c")
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})
}
