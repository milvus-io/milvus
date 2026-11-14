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
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// BcryptMaxPasswordBytes is the maximum plaintext length representable by bcrypt.
const BcryptMaxPasswordBytes = 72

// VerifyStoredPassword compares a plaintext password with a stored hash. The
// algorithm comes from the format of the hash rather than from configuration,
// so a credential keeps working after common.security.passwordHashAlgorithm
// changes and on a CDC secondary configured differently from its primary.
// A candidate that bcrypt cannot represent is a mismatch just like one that
// does not match. A malformed stored hash is instead a credential-store failure
// and must not be reported as a bad password, or an operator holding the right
// one goes looking for the wrong problem.
func VerifyStoredPassword(ctx context.Context, storedHash, password string) error {
	if strings.HasPrefix(storedHash, pbkdf2SHA512Prefix) {
		return verifyPBKDF2SHA512(storedHash, password)
	}
	if len(password) > BcryptMaxPasswordBytes {
		return merr.WrapErrPrivilegeNotAuthenticated("invalid password")
	}
	err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password))
	switch {
	case err == nil:
		if paramtable.Get().CommonCfg.PasswordHashAlgorithm.GetValue() == PasswordHashPBKDF2SHA512 {
			// Rated: every login of a user who has not rotated yet lands here.
			mlog.RatedWarn(ctx, rate.Every(time.Minute),
				"verified a bcrypt password hash while pbkdf2-sha512 is configured; update this password to rehash it")
		}
		return nil
	case errors.Is(err, bcrypt.ErrMismatchedHashAndPassword):
		return merr.WrapErrPrivilegeNotAuthenticated("invalid password")
	default:
		return merr.WrapErrServiceInternalErr(err, "stored credential hash is invalid")
	}
}
