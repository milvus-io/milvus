//go:build test
// +build test

/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hookutil

import "github.com/milvus-io/milvus-proto/go-api/v2/hook"

// MockAPIHook is a mock hook for api key verification, ONLY FOR TEST
type MockAPIHook struct {
	DefaultHook
	MockErr error
	User    string
}

func (m MockAPIHook) VerifyAPIKey(apiKey string) (string, error) {
	return m.User, m.MockErr
}

func SetMockAPIHook(apiUser string, mockErr error) {
	if apiUser == "" && mockErr == nil {
		storeHook(&DefaultHook{})
		return
	}
	storeHook(&MockAPIHook{
		MockErr: mockErr,
		User:    apiUser,
	})
}

func SetTestHook(hookVal hook.Hook) {
	storeHook(hookVal)
}

func SetTestExtension(extVal hook.Extension) {
	storeExtension(extVal)
}
