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

	t.Run("no CA configured returns nil", func(t *testing.T) {
		paramtable.Get().Save("tls.caPemPath", "")
		paramtable.Get().Save("tls.clientPemPath", "")
		paramtable.Get().Save("tls.clientKeyPath", "")

		tlsConfig, err := buildCDCTLSConfig()
		assert.NoError(t, err)
		assert.Nil(t, tlsConfig)
	})

	t.Run("invalid CA path returns error", func(t *testing.T) {
		paramtable.Get().Save("tls.caPemPath", "/nonexistent/ca.pem")
		defer paramtable.Get().Save("tls.caPemPath", "")

		_, err := buildCDCTLSConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA cert")
	})
}
