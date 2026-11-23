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
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#include <openssl/rand.h>
#include <string.h>

static int randUsesFIPSProvider(EVP_RAND_CTX *ctx) {
	EVP_RAND *rand = ctx != NULL ? EVP_RAND_CTX_get0_rand(ctx) : NULL;
	const OSSL_PROVIDER *provider = rand != NULL ? EVP_RAND_get0_provider(rand) : NULL;
	return provider != NULL && strcmp(OSSL_PROVIDER_get0_name(provider), "fips") == 0;
}

static int enableOpenSSLFIPS(void) {
	OSSL_LIB_CTX *libctx = OSSL_LIB_CTX_get0_global_default();
	unsigned char buf[1];

	if (libctx == NULL || !OSSL_LIB_CTX_load_config(libctx, "/milvus/configs/ssl/openssl-fips.cnf")) {
		return -1;  // config load failed
	}
	if (!EVP_default_properties_is_fips_enabled(libctx)) {
		return 0;   // config loaded but fips=yes not set
	}
	// Properties govern future fetches. An embedding host may already have
	// initialized the shared primary or this thread's child generators.
	if (!randUsesFIPSProvider(RAND_get0_primary(libctx)) ||
	    !randUsesFIPSProvider(RAND_get0_public(libctx)) ||
	    !randUsesFIPSProvider(RAND_get0_private(libctx))) {
		return -3;
	}
	if (RAND_bytes_ex(libctx, buf, sizeof(buf), 0) != 1 ||
	    RAND_priv_bytes_ex(libctx, buf, sizeof(buf), 0) != 1) {
		return -2;  // FIPS property set but provider not functional
	}
	return 1;       // FIPS truly enabled and functional
}
*/
import "C"

import (
	"context"
	"crypto/fips140"
	"runtime/debug"
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
		case -3:
			mlog.Error(ctx, "OpenSSL random generator is unavailable or uses a provider other than fips; check OPENSSL_MODULES and enable FIPS before initializing OpenSSL random generators")
		case -2:
			mlog.Error(ctx, "OpenSSL FIPS random generation failed")
		default:
			mlog.Error(ctx, "Failed to load OpenSSL FIPS config /milvus/configs/ssl/openssl-fips.cnf")
		}
	})
	return opensslFIPSEnabled
}

// Go 1.26.10's certified snapshot for CMVP certificate #5247. The public
// fips140.Version() omits the suffix that identifies the exact snapshot.
const certifiedGoSnapshot = "v1.0.0-c2097c7c"

func requireGoFIPS(info *debug.BuildInfo, enabled bool) error {
	var snapshot string
	if info != nil {
		for _, setting := range info.Settings {
			if setting.Key == "GOFIPS140" {
				snapshot = setting.Value
				break
			}
		}
	}
	if snapshot != certifiedGoSnapshot {
		return merr.WrapErrServiceInternalMsg("FIPS build requires Go Cryptographic Module snapshot %q, binary records GOFIPS140=%q", certifiedGoSnapshot, snapshot)
	}
	if !enabled {
		return merr.WrapErrServiceInternalMsg("FIPS build, but Go FIPS 140-3 mode is off; do not set GODEBUG=fips140=off")
	}
	return nil
}

// Require checks the Go module and mode before enabling the OpenSSL provider.
func Require() error {
	info, _ := debug.ReadBuildInfo()
	if err := requireGoFIPS(info, fips140.Enabled()); err != nil {
		return err
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
