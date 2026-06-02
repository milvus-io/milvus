//go:build boringcrypto

package milvus

import (
	"crypto/boring"

	"github.com/milvus-io/milvus/pkg/v3/util/fips"
)

func boringEnabled() bool {
	return boring.Enabled()
}

func maybeEnableOpenSSLFIPS() bool {
	return fips.MaybeEnableOpenSSLFIPS()
}
