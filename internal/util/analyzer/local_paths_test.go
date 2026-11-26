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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveLocalPaths(t *testing.T) {
	params := `{"tokenizer":{"type":"language_identifier","analyzers":{"default":{"tokenizer":{"type":"grpc","endpoint":"https://host","tls":{"ca_cert":"ca.pem","client_cert":"/old/client.pem","client_key":"key.pem"}}}}},"filter":[{"type":"stop","stop_words_file":{"type":"local","path":"nested/stop.txt"}}],"large_integer":9007199254740993}`
	resolved, err := ResolveLocalPaths(params, "/dictionary", "/credentials")
	require.NoError(t, err)
	require.Contains(t, resolved, `"path":"/dictionary/nested/stop.txt"`)
	require.Contains(t, resolved, `"ca_cert":"/credentials/ca.pem"`)
	require.Contains(t, resolved, `"client_cert":"/old/client.pem"`)
	require.Contains(t, resolved, `"client_key":"/credentials/key.pem"`)
	require.Contains(t, resolved, `9007199254740993`)
	again, err := ResolveLocalPaths(resolved, "/new-dictionary", "/new-credentials")
	require.NoError(t, err)
	require.JSONEq(t, resolved, again)
	_, err = ResolveLocalPaths(`{"filter":[{"stop_words_file":{"type":"local","path":"../secret"}}]}`, "/dictionary", "/credentials")
	require.Error(t, err)
}
