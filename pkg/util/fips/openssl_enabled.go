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

//go:build milvus_fips

package fips

/*
#cgo LDFLAGS: -lcrypto
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#include <openssl/rand.h>

static int enableOpenSSLFIPS() {
	unsigned char buf[1];

	if (!OSSL_LIB_CTX_load_config(NULL, "/milvus/configs/ssl/openssl-fips.cnf")) {
		return -1;  // config load failed
	}
	if (!EVP_default_properties_is_fips_enabled(NULL)) {
		return 0;   // config loaded but fips=yes not set
	}
	// EVP_default_properties_is_fips_enabled only checks the property string,
	// not whether the FIPS provider is actually loaded. Verify by checking
	// that RAND_bytes works (requires a working FIPS DRBG).
	if (RAND_bytes(buf, 1) != 1) {
		return -2;  // FIPS property set but provider not functional
	}
	return 1;       // FIPS truly enabled and functional
}
*/
import "C"

import (
	"context"
	"crypto/fips140"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var (
	fipsOnce           sync.Once
	opensslFIPSEnabled bool
)

// MaybeEnableOpenSSLFIPS loads OpenSSL's FIPS configuration once for the
// process and reports whether the FIPS provider is in use.
func MaybeEnableOpenSSLFIPS() bool {
	fipsOnce.Do(func() {
		ctx := context.TODO()
		switch C.enableOpenSSLFIPS() {
		case 1:
			mlog.Info(ctx, "OpenSSL FIPS mode enabled")
			opensslFIPSEnabled = true
		case 0:
			mlog.Error(ctx, "OpenSSL FIPS config loaded but fips=yes not set in default properties")
		case -2:
			mlog.Error(ctx, "OpenSSL FIPS property set but FIPS provider not functional (check OPENSSL_MODULES env var and .include path in openssl-fips.cnf)")
		default:
			mlog.Error(ctx, "Failed to load OpenSSL FIPS config /milvus/configs/ssl/openssl-fips.cnf")
		}
	})
	return opensslFIPSEnabled
}

// Require returns an error unless both halves of a MILVUS_FIPS_ENABLED=ON
// build are in FIPS 140-3 mode: Go's cryptography on a validated version of
// the Go Cryptographic Module, and OpenSSL with its FIPS provider.
func Require() error {
	// A frozen module reports its version; "latest" is the standard library's
	// own copy, which is not the validated module.
	if version := fips140.Version(); version == "latest" {
		return merr.WrapErrServiceInternalMsg("FIPS build, but Go was not built against a validated Go Cryptographic Module; build with MILVUS_FIPS_ENABLED=ON so that GOFIPS140 is set")
	}
	if !fips140.Enabled() {
		return merr.WrapErrServiceInternalMsg("FIPS build, but Go FIPS 140-3 mode is off; do not set GODEBUG=fips140=off")
	}
	if !MaybeEnableOpenSSLFIPS() {
		return merr.WrapErrServiceInternalMsg("FIPS build, but the OpenSSL FIPS provider could not be enabled; see the error logged above")
	}
	return nil
}

// Status describes the FIPS state of the process for the startup banner.
func Status() string {
	return "on (Go Cryptographic Module " + fips140.Version() + ", OpenSSL FIPS provider)"
}
