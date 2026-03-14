//go:build boringcrypto

package milvus

import "crypto/boring"

func boringEnabled() bool {
	return boring.Enabled()
}
