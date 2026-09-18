package message

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// messageWithoutCipherHeader is a message implementation that cannot carry a
// cipher header. Embedding the interface promotes no cipherHeader method.
type messageWithoutCipherHeader struct{ BasicMessage }

func TestCipherConfigOf(t *testing.T) {
	require.Nil(t, CipherConfigOf(nil))

	plain := CreateTestEmptyInsertMesage(1, nil)
	require.Nil(t, CipherConfigOf(plain))

	header, err := EncodeProto(&messagespb.CipherHeader{EzId: 3, CollectionId: 9})
	require.NoError(t, err)
	properties := make(map[string]string, len(plain.Properties().ToRawMap())+1)
	for k, v := range plain.Properties().ToRawMap() {
		properties[k] = v
	}
	properties[messageCipherHeader] = header
	encrypted := NewMutableMessageBeforeAppend(plain.Payload(), properties)

	cfg := CipherConfigOf(encrypted)
	require.NotNil(t, cfg)
	require.Equal(t, int64(3), cfg.EzID)
	require.Equal(t, int64(9), cfg.CollectionID)
	require.Equal(t, cfg, CipherConfigOf(MustAsMutableInsertMessageV1(encrypted)))

	require.Panics(t, func() {
		CipherConfigOf(messageWithoutCipherHeader{})
	}, "a message that cannot carry a cipher header must not be reported as plaintext")
}
