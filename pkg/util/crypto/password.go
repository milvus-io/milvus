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

package crypto

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// BcryptMaxPasswordBytes is the maximum plaintext length representable by bcrypt.
// Every algorithm enforces it, so a password stays valid if the algorithm changes.
const BcryptMaxPasswordBytes = 72

// errBcryptMismatch is returned by bcryptVerify when the password does not match.
var errBcryptMismatch = errors.New("bcrypt: password does not match")

// VerifyStoredPassword compares a plaintext password with a stored hash. The
// algorithm comes from the format of the hash rather than from configuration,
// so a credential keeps working after common.security.passwordHashAlgorithm
// changes and on a CDC secondary configured differently from its primary.
// A candidate that bcrypt cannot represent is a mismatch just like one that
// does not match. A malformed stored hash is instead a credential-store failure
// and must not be reported as a bad password, or an operator holding the right
// one goes looking for the wrong problem.
// IsPBKDF2SHA512Hash reports whether a stored hash is in the PBKDF2-SHA512
// format. Any other stored hash is bcrypt.
func IsPBKDF2SHA512Hash(storedHash string) bool {
	return strings.HasPrefix(storedHash, pbkdf2SHA512Prefix)
}

func VerifyStoredPassword(ctx context.Context, storedHash, password string) error {
	if IsPBKDF2SHA512Hash(storedHash) {
		return verifyPBKDF2SHA512(storedHash, password)
	}
	if !BcryptAvailable {
		// Any other hash is bcrypt, which this build cannot verify.
		mlog.RatedWarn(ctx, rate.Every(time.Minute),
			"a stored password hash uses bcrypt, which this build leaves out; the user cannot log in until the password is reset")
		return bcryptVerify(storedHash, password)
	}
	if len(password) > BcryptMaxPasswordBytes {
		return merr.WrapErrPrivilegeNotAuthenticated("invalid password")
	}
	err := bcryptVerify(storedHash, password)
	switch {
	case err == nil:
		if paramtable.Get().CommonCfg.PasswordHashAlgorithm.GetValue() == PasswordHashPBKDF2SHA512 {
			// Rated: every login of a user who has not rotated yet lands here.
			mlog.RatedWarn(ctx, rate.Every(time.Minute),
				"verified a bcrypt password hash while pbkdf2-sha512 is configured; update this password to rehash it")
		}
		return nil
	case errors.Is(err, errBcryptMismatch):
		return merr.WrapErrPrivilegeNotAuthenticated("invalid password")
	default:
		return merr.WrapErrServiceInternalErr(err, "stored credential hash is invalid")
	}
}
