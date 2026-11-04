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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	pbkdf2SHA512Prefix = "$" + PasswordHashPBKDF2SHA512 + "$"

	pbkdf2SHA512Iterations = 210000
	pbkdf2SHA512SaltBytes  = 16
	pbkdf2SHA512KeyBytes   = 64

	// Bounds on the iteration count a stored hash may declare. The count is
	// read from the hash so that it can be raised later without invalidating
	// existing credentials, but a corrupt value must not pin a comparison slot
	// on the CPU or silently reduce the work to almost nothing. The floor is
	// the NIST SP 800-132 minimum.
	pbkdf2SHA512MinIterations = 1000
	pbkdf2SHA512MaxIterations = 10000000
)

// pbkdf2Encoding omits padding, as the PHC string format this layout follows does.
var pbkdf2Encoding = base64.RawStdEncoding.Strict()

// pbkdf2SHA512 implements PBKDF2 (RFC 8018, section 5.2) with HMAC-SHA-512.
//
// This is deliberately not the standard library's crypto/pbkdf2: that package
// always runs on Go's native FIPS module, while crypto/hmac and crypto/sha512
// route to BoringCrypto under GOEXPERIMENT=boringcrypto, which is what
// MILVUS_FIPS_ENABLED=ON builds with.
func pbkdf2SHA512(password, salt []byte, iterations, keyBytes int) []byte {
	prf := hmac.New(sha512.New, password)
	hashBytes := prf.Size()
	blocks := (keyBytes + hashBytes - 1) / hashBytes

	derived := make([]byte, 0, blocks*hashBytes)
	u := make([]byte, 0, hashBytes)
	t := make([]byte, hashBytes)
	var blockIndex [4]byte
	for block := 1; block <= blocks; block++ {
		binary.BigEndian.PutUint32(blockIndex[:], uint32(block))
		prf.Reset()
		prf.Write(salt)
		prf.Write(blockIndex[:])
		u = prf.Sum(u[:0])
		copy(t, u)
		for i := 2; i <= iterations; i++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			subtle.XORBytes(t, t, u)
		}
		derived = append(derived, t...)
	}
	return derived[:keyBytes]
}

// encryptPBKDF2SHA512 returns $pbkdf2-sha512$i=<iterations>$<base64 salt>$<base64 key>.
func encryptPBKDF2SHA512(pwd string) (string, error) {
	// The same limit bcrypt enforces, so that the algorithm in use does not
	// decide which passwords are acceptable. proxy.maxPasswordLength and the
	// management-plane verifier both assume it.
	if len(pwd) > BcryptMaxPasswordBytes {
		return "", merr.WrapErrParameterInvalidMsg("password length must not exceed %d bytes", BcryptMaxPasswordBytes)
	}
	salt := make([]byte, pbkdf2SHA512SaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "failed to generate password salt")
	}
	key := pbkdf2SHA512([]byte(pwd), salt, pbkdf2SHA512Iterations, pbkdf2SHA512KeyBytes)
	return fmt.Sprintf("%si=%d$%s$%s", pbkdf2SHA512Prefix, pbkdf2SHA512Iterations,
		pbkdf2Encoding.EncodeToString(salt), pbkdf2Encoding.EncodeToString(key)), nil
}

// verifyPBKDF2SHA512 follows the contract of VerifyStoredPassword. Parse
// failures use fixed reasons because the error reaches logs and the hash, even
// a damaged one, must not.
func verifyPBKDF2SHA512(storedHash, password string) error {
	iterations, salt, key, reason := parsePBKDF2SHA512(storedHash)
	if reason != "" {
		return merr.WrapErrServiceInternalMsg("stored credential hash is invalid: %s", reason)
	}
	derived := pbkdf2SHA512([]byte(password), salt, iterations, len(key))
	if subtle.ConstantTimeCompare(derived, key) != 1 {
		return merr.WrapErrPrivilegeNotAuthenticated("invalid password")
	}
	return nil
}

// parsePBKDF2SHA512 returns a non-empty reason when storedHash is malformed.
func parsePBKDF2SHA512(storedHash string) (iterations int, salt, key []byte, reason string) {
	fields := strings.Split(strings.TrimPrefix(storedHash, pbkdf2SHA512Prefix), "$")
	if len(fields) != 3 {
		return 0, nil, nil, "unexpected number of fields"
	}
	rawIterations, ok := strings.CutPrefix(fields[0], "i=")
	if !ok {
		return 0, nil, nil, "missing iteration count"
	}
	iterations, err := strconv.Atoi(rawIterations)
	// The round trip rejects spellings such as "+210000" or "0210000".
	if err != nil || strconv.Itoa(iterations) != rawIterations {
		return 0, nil, nil, "iteration count is not a number"
	}
	if iterations < pbkdf2SHA512MinIterations || iterations > pbkdf2SHA512MaxIterations {
		return 0, nil, nil, "iteration count is out of range"
	}
	salt, err = pbkdf2Encoding.DecodeString(fields[1])
	if err != nil || len(salt) < pbkdf2SHA512SaltBytes {
		return 0, nil, nil, "invalid salt"
	}
	key, err = pbkdf2Encoding.DecodeString(fields[2])
	if err != nil || len(key) != pbkdf2SHA512KeyBytes {
		return 0, nil, nil, "invalid derived key"
	}
	return iterations, salt, key, ""
}
