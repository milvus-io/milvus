//go:build !boringcrypto

package milvus

import "github.com/milvus-io/milvus/pkg/v3/util/fips"

func boringEnabled() bool {
	return false
}

func maybeEnableOpenSSLFIPS() bool {
	return fips.MaybeEnableOpenSSLFIPS()
}
