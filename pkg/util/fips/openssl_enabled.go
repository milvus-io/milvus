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
	"log"
	"sync"
)

var (
	fipsOnce           sync.Once
	opensslFIPSEnabled bool
)

func MaybeEnableOpenSSLFIPS() bool {
	fipsOnce.Do(func() {
		rc := C.enableOpenSSLFIPS()
		switch rc {
		case 1:
			log.Println("OpenSSL FIPS mode enabled")
			opensslFIPSEnabled = true
		case 0:
			log.Println("WARNING: OpenSSL FIPS config loaded but fips=yes not set in default properties")
		case -2:
			log.Println("FATAL: OpenSSL FIPS property set but FIPS provider not functional (check OPENSSL_MODULES env var and .include path in openssl-fips.cnf)")
		default:
			log.Println("Failed to load OpenSSL FIPS config")
		}
	})
	return opensslFIPSEnabled
}
