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
//go:build !nobcrypt

package crypto

import (
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/bcrypt"
)

// BcryptAvailable reports whether this binary can hash and verify bcrypt
// passwords. It is false only in builds with -tags nobcrypt.
const BcryptAvailable = true

func bcryptHash(pwd string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// bcryptVerify returns errBcryptMismatch when the password does not match,
// and any other error when the stored hash cannot be used.
func bcryptVerify(storedHash, password string) error {
	err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password))
	if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		return errBcryptMismatch
	}
	return err
}
