//go:build boringcrypto

package milvus

/*
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
	"crypto/boring"
	"log"
	"sync"
)

func boringEnabled() bool {
	return boring.Enabled()
}

var fipsOnce sync.Once

func maybeEnableOpenSSLFIPS() {
	fipsOnce.Do(func() {
		rc := C.enableOpenSSLFIPS()
		switch rc {
		case 1:
			log.Println("OpenSSL FIPS mode enabled")
		case 0:
			log.Println("WARNING: OpenSSL FIPS config loaded but fips=yes not set in default properties")
		case -2:
			log.Println("FATAL: OpenSSL FIPS property set but FIPS provider not functional (check OPENSSL_MODULES env var and .include path in openssl-fips.cnf)")
		default:
			log.Println("Failed to load OpenSSL FIPS config")
		}
	})
}
