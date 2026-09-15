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

package packed

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestMakePropertiesFromStorageConfig_RootPath(t *testing.T) {
	for _, tc := range []struct {
		storageType string
		rootPath    string
		expected    string
	}{
		{storageType: "local", rootPath: "/var/lib/milvus/data", expected: "/"},
		{storageType: "remote", rootPath: "files", expected: "files"},
	} {
		t.Run(tc.storageType, func(t *testing.T) {
			props, err := MakePropertiesFromStorageConfig(&indexpb.StorageConfig{
				StorageType: tc.storageType,
				RootPath:    tc.rootPath,
			}, nil)
			require.NoError(t, err)
			defer FreeProperties(props)
			assert.Equal(t, tc.expected, loonPropertyString(props, PropertyFSRootPath))
		})
	}
}
