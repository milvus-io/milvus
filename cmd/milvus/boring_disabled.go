//go:build !boringcrypto

package milvus

func boringEnabled() bool {
	return false
}

func maybeEnableOpenSSLFIPS() {}
