package message

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// CipherConfigOf returns the cipher config that msg was encrypted with,
// or nil if msg is not encrypted.
// It panics for a message implementation that cannot carry a cipher header,
// because a silent nil would send a derived message of an encrypted collection
// out as plaintext.
func CipherConfigOf(msg BasicMessage) *CipherConfig {
	if msg == nil {
		return nil
	}
	c, ok := msg.(interface {
		cipherHeader() *messagespb.CipherHeader
	})
	if !ok {
		panic(fmt.Sprintf("message implementation %T does not carry a cipher header", msg))
	}
	header := c.cipherHeader()
	if header == nil {
		return nil
	}
	return &CipherConfig{
		EzID:         header.GetEzId(),
		CollectionID: header.GetCollectionId(),
	}
}
