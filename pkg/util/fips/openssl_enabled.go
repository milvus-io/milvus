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

//go:build boringcrypto

package fips

/*
#cgo LDFLAGS: -lssl -lcrypto
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <stdlib.h>

static int enableOpenSSLFIPS(const char *config, int skip_system_config) {
	unsigned char buf[1];

	// libssl loads the system openssl.cnf the first time it initialises, and that
	// file's [ssl_conf] replaces the one in config. Initialising libssl here
	// without loading any config first makes config the only source of TLS
	// defaults. If libssl was already initialised, this call does nothing, and
	// loading config below still applies its [ssl_conf].
	if (skip_system_config && !OPENSSL_init_ssl(OPENSSL_INIT_NO_LOAD_CONFIG, NULL)) {
		return -1;
	}
	if (!OSSL_LIB_CTX_load_config(NULL, config)) {
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
	"fmt"
	"log"
	"sync"
	"unsafe"
)

var (
	fipsOnce           sync.Once
	opensslFIPSEnabled bool
)

func MaybeEnableOpenSSLFIPS() bool {
	fipsOnce.Do(func() {
		var err error
		opensslFIPSEnabled, err = enableOpenSSLFIPSFromConfig(opensslFIPSConfigPath)
		if err != nil {
			log.Fatal(err)
		}
	})
	return opensslFIPSEnabled
}

// enableOpenSSLFIPSFromConfig reports whether OpenSSL FIPS mode is enabled. A
// strict build promises FIPS-only crypto, so it returns an error instead of
// falling back to running without it the way a MILVUS_FIPS_ENABLED=ON build does.
func enableOpenSSLFIPSFromConfig(path string) (bool, error) {
	config := C.CString(path)
	defer C.free(unsafe.Pointer(config))
	enabled := false
	// Only a strict build skips the system openssl.cnf; its own config carries
	// the TLS defaults instead (key-exchange groups, minimum version).
	skipSystemConfig := C.int(0)
	if StrictMode() {
		skipSystemConfig = 1
	}
	switch C.enableOpenSSLFIPS(config, skipSystemConfig) {
	case 1:
		log.Println("OpenSSL FIPS mode enabled")
		enabled = true
	case 0:
		log.Println("WARNING: OpenSSL FIPS config loaded but fips=yes not set in default properties")
	case -2:
		log.Println("FATAL: OpenSSL FIPS property set but FIPS provider not functional (check OPENSSL_MODULES env var and .include path in openssl-fips.cnf)")
	default:
		log.Println("Failed to load OpenSSL FIPS config")
	}
	if !enabled && StrictMode() {
		//nolint:gocritic // paramtable imports this package, so it stays free of Milvus dependencies
		return false, fmt.Errorf("MILVUS_FIPS_STRICT build could not enable OpenSSL FIPS mode using %s", path)
	}
	return enabled, nil
}
