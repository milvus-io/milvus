// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//go:build nobcrypt

package paramtable

// defaultPasswordHashAlgorithm is the default of
// common.security.passwordHashAlgorithm (pbkdf2-sha512: builds with -tags nobcrypt leave bcrypt out).
const defaultPasswordHashAlgorithm = "pbkdf2-sha512" //nolint:gosec // algorithm name, not a credential

// formatPasswordHashAlgorithm maps bcrypt to pbkdf2-sha512. configs/milvus.yaml
// sets bcrypt explicitly, and this build cannot produce bcrypt hashes, so it
// hashes with pbkdf2-sha512 instead; rootcoord logs this at startup. Other
// values are left for crypto.PasswordEncrypt to accept or reject.
func formatPasswordHashAlgorithm(v string) string {
	if v == "bcrypt" {
		return "pbkdf2-sha512"
	}
	return v
}
