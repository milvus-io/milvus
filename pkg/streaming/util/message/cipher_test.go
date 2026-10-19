package message

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// mockCipher is a simple mock implementation for testing
type mockCipher struct {
	getDecryptorFunc func(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error)
	getEncryptorFunc func(ezID, collectionID int64) (hook.Encryptor, []byte, error)
	getUnsafeKeyFunc func(ezID, collectionID int64) []byte
	initFunc         func(params map[string]string) error
}

func (m *mockCipher) Init(params map[string]string) error {
	if m.initFunc != nil {
		return m.initFunc(params)
	}
	return nil
}

func (m *mockCipher) GetEncryptor(ezID, collectionID int64) (hook.Encryptor, []byte, error) {
	if m.getEncryptorFunc != nil {
		return m.getEncryptorFunc(ezID, collectionID)
	}
	return nil, nil, nil
}

func (m *mockCipher) GetDecryptor(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	if m.getDecryptorFunc != nil {
		return m.getDecryptorFunc(ezID, collectionID, safeKey)
	}
	return nil, nil
}

func (m *mockCipher) GetUnsafeKey(ezID, collectionID int64) []byte {
	if m.getUnsafeKeyFunc != nil {
		return m.getUnsafeKeyFunc(ezID, collectionID)
	}
	return nil
}

// mockDecryptor is a simple mock decryptor
type mockDecryptor struct {
	decryptFunc func([]byte) ([]byte, error)
}

func (m *mockDecryptor) Decrypt(data []byte) ([]byte, error) {
	if m.decryptFunc != nil {
		return m.decryptFunc(data)
	}
	return data, nil
}

func TestDecodePayloadReturnsDecryptErrors(t *testing.T) {
	origCipher := cipher
	t.Cleanup(func() { cipher = origCipher })

	encodedHeader, err := EncodeProto(&messagespb.CipherHeader{
		EzId:         1,
		CollectionId: 10,
		SafeKey:      []byte("safe-key"),
	})
	assert.NoError(t, err)
	msg := &messageImpl{
		payload: []byte("ciphertext"),
		properties: propertiesImpl{
			messageCipherHeader: encodedHeader,
		},
	}

	t.Run("cipher unavailable", func(t *testing.T) {
		cipher = nil
		_, err := msg.decodePayload(context.Background())
		assert.ErrorContains(t, err, "cipher not registered")
	})

	t.Run("get decryptor", func(t *testing.T) {
		expected := errors.New("decryptor unavailable")
		cipher = &mockCipher{
			getDecryptorFunc: func(int64, int64, []byte) (hook.Decryptor, error) {
				return nil, expected
			},
		}
		_, err := msg.decodePayload(context.Background())
		assert.ErrorIs(t, err, expected)
	})

	t.Run("decrypt", func(t *testing.T) {
		expected := errors.New("decrypt failed")
		cipher = &mockCipher{
			getDecryptorFunc: func(int64, int64, []byte) (hook.Decryptor, error) {
				return &mockDecryptor{
					decryptFunc: func([]byte) ([]byte, error) {
						return nil, expected
					},
				}, nil
			},
		}
		_, err := msg.decodePayload(context.Background())
		assert.ErrorIs(t, err, expected)
	})

	t.Run("context canceled during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cipher = &mockCipher{
			getDecryptorFunc: func(int64, int64, []byte) (hook.Decryptor, error) {
				cancel()
				return nil, ErrKmsKeyInvalid
			},
		}
		_, err := msg.decodePayload(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestIsKmsKeyInvalidError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "exact error",
			err:      ErrKmsKeyInvalid,
			expected: true,
		},
		{
			name:     "wrapped error",
			err:      fmt.Errorf("%w: additional context", ErrKmsKeyInvalid),
			expected: true,
		},
		{
			name:     "error from plugin with matching message",
			err:      errors.New("kms key invalid"),
			expected: true,
		},
		{
			name:     "error from plugin with message in context",
			err:      errors.New("failed to decrypt: kms key invalid: permission denied"),
			expected: true,
		},
		{
			name:     "different error",
			err:      errors.New("some other error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isKmsKeyInvalidError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
