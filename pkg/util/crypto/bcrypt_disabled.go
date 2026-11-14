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

package crypto

import (
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// BcryptAvailable reports whether this binary can hash and verify bcrypt
// passwords. It is false only in builds with -tags nobcrypt.
const BcryptAvailable = false

func bcryptHash(string) (string, error) {
	return "", merr.WrapErrServiceInternalMsg(
		"common.security.passwordHashAlgorithm=bcrypt is not available in this build (built with -tags nobcrypt); use pbkdf2-sha512")
}

// bcryptVerify explains a stored bcrypt hash in a build that cannot verify it.
// It is a credential-store problem, not a wrong password: the password has to
// be reset, or the hash replaced, before this user can log in.
func bcryptVerify(string, string) error {
	return merr.WrapErrServiceInternalMsg(
		"stored password hash uses bcrypt, which this build leaves out (built with -tags nobcrypt); " +
			"update the password on a build with bcrypt and passwordHashAlgorithm=pbkdf2-sha512 before switching, " +
			"or replace the credential")
}
