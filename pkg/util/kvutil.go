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

package util

import (
	"strings"
)

// GetPath joins rootPath and key with a "/" separator for KV storage operations.
// Unlike path.Join, it preserves trailing "/" in key, which is semantically significant
// for prefix-based queries in etcd/TiKV (e.g. "user/" must not match "user_data/...").
func GetPath(rootPath, key string) string {
	if len(key) == 0 {
		return rootPath
	}
	if len(rootPath) == 0 {
		return key
	}
	if strings.HasSuffix(rootPath, "/") && strings.HasPrefix(key, "/") {
		return rootPath + key[1:]
	}
	if !strings.HasSuffix(rootPath, "/") && !strings.HasPrefix(key, "/") {
		return rootPath + "/" + key
	}
	return rootPath + key
}
