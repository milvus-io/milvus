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

package credentials

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAPIKeyCredentialWithMixedCaseName(t *testing.T) {
	credentials := NewCredentials(map[string]string{ //nolint:gosec // not a real credential, test data
		"aistaffhub_credential.apikey": "mock-api-key",
	})

	apiKey, err := credentials.GetAPIKeyCredential("aIstaffhub_credential")

	require.NoError(t, err)
	assert.Equal(t, "mock-api-key", apiKey)
}

func TestGetAKSKCredentialWithMixedCaseName(t *testing.T) {
	credentials := NewCredentials(map[string]string{ //nolint:gosec // not real credentials, test data
		"aistaffhub_credential.access_key_id":     "mock-access-key-id",
		"aistaffhub_credential.secret_access_key": "mock-secret-access-key",
	})

	accessKeyID, secretAccessKey, err := credentials.GetAKSKCredential("aIstaffhub_credential")

	require.NoError(t, err)
	assert.Equal(t, "mock-access-key-id", accessKeyID)
	assert.Equal(t, "mock-secret-access-key", secretAccessKey)
}

func TestGetGcpCredentialWithMixedCaseName(t *testing.T) {
	credentialsJSON := []byte(`{"type":"service_account"}`)
	credentials := NewCredentials(map[string]string{
		"aistaffhub_credential.credential_json": base64.StdEncoding.EncodeToString(credentialsJSON),
	})

	actual, err := credentials.GetGcpCredential("aIstaffhub_credential")

	require.NoError(t, err)
	assert.Equal(t, credentialsJSON, actual)
}
