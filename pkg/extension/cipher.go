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

package extension

import "github.com/milvus-io/milvus-proto/go-api/v3/hook"

// SetCipher installs a compiled-in cipher: the Go half of the cipher plug-in
// pair, which hookutil prefers over cipherPlugin.soPathGo and refuses beside
// it. Only that half: the C++ half is still loaded by the core from
// cipherPlugin.soPathCpp, and that path stays the deployment's declaration
// that encryption is on - a compiled-in cipher with no soPathCpp is left
// idle, exactly as no plug-in would be loaded. Call it before milvus starts;
// nil installs nothing. It is independent of SetHook and of FormInstalled.
func SetCipher(c hook.Cipher) {
	installedCipher.Store(&cipherBox{cipher: c})
}

// InstalledCipher returns the installed cipher, or nil.
func InstalledCipher() hook.Cipher {
	if b := installedCipher.Load(); b != nil {
		return b.cipher
	}
	return nil
}
