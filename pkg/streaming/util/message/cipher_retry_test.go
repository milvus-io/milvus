package message

import (
	"context"
	"net"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDecryptPayloadRetry(t *testing.T) {
	for _, stage := range []string{"get decryptor", "decrypt"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				getCipher := mockey.Mock(getCipher).Return(&mockCipher{}, nil).Build()
				defer getCipher.UnPatch()
				// Exercise one backoff sequence across different transient failures.
				failures := []error{
					ErrKmsKeyInvalid, status.Error(codes.Unavailable, "KMS unavailable"),
					merr.ErrServiceUnavailable, context.DeadlineExceeded,
					syscall.ECONNRESET, status.Error(codes.ResourceExhausted, "KMS throttled"),
					ErrKmsKeyInvalid,
				}
				var attempts []time.Time
				nextError := func() error {
					attempts = append(attempts, time.Now())
					if len(attempts) <= len(failures) {
						return failures[len(attempts)-1]
					}
					return nil
				}
				getDecryptor := mockey.Mock((*mockCipher).GetDecryptor).To(func(_ *mockCipher, ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
					require.EqualValues(t, 1, ezID)
					require.EqualValues(t, 10, collectionID)
					require.Equal(t, []byte("safe-key"), safeKey)
					if stage == "get decryptor" {
						if err := nextError(); err != nil {
							return nil, err
						}
					}
					return &mockDecryptor{}, nil
				}).Build()
				defer getDecryptor.UnPatch()
				decrypt := mockey.Mock((*mockDecryptor).Decrypt).To(func(_ *mockDecryptor, payload []byte) ([]byte, error) {
					require.Equal(t, []byte("ciphertext"), payload)
					if stage == "decrypt" {
						if err := nextError(); err != nil {
							return []byte("partial result"), err
						}
					}
					return []byte("plaintext"), nil
				}).Build()
				defer decrypt.UnPatch()
				header, err := EncodeProto(&messagespb.CipherHeader{EzId: 1, CollectionId: 10, SafeKey: []byte("safe-key")})
				require.NoError(t, err)
				msg := &messageImpl{payload: []byte("ciphertext"), properties: propertiesImpl{messageCipherHeader: header}}
				decoded, err := msg.decodePayload(context.Background())
				require.NoError(t, err)
				require.Equal(t, []byte("plaintext"), decoded)
				require.Equal(t, 8, getDecryptor.Times(), "each retry must reacquire the decryptor")
				require.Len(t, attempts, 8)
				for i, delay := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3 * time.Second, 3 * time.Second} {
					require.Equal(t, delay, attempts[i+1].Sub(attempts[i]))
				}
			})
		})
	}
}

func TestDecryptPayloadRetryCancellation(t *testing.T) {
	for _, stage := range []string{"get decryptor", "decrypt"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				getCipher := mockey.Mock(getCipher).Return(&mockCipher{}, nil).Build()
				defer getCipher.UnPatch()
				getDecryptor := mockey.Mock((*mockCipher).GetDecryptor).To(func(_ *mockCipher, _, _ int64, _ []byte) (hook.Decryptor, error) {
					if stage == "get decryptor" {
						return nil, merr.ErrServiceUnavailable
					}
					return &mockDecryptor{}, nil
				}).Build()
				defer getDecryptor.UnPatch()
				decrypt := mockey.Mock((*mockDecryptor).Decrypt).Return(nil, merr.ErrServiceUnavailable).Build()
				defer decrypt.UnPatch()
				ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				defer cancel()
				_, err := decryptPayloadWithRetry(ctx, 1, 10, nil, nil)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, 2, getDecryptor.Times())
			})
		})
	}
}

func TestDecryptPayloadCanceledBeforeDecrypt(t *testing.T) {
	getCipher := mockey.Mock(getCipher).Return(&mockCipher{}, nil).Build()
	defer getCipher.UnPatch()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	getDecryptor := mockey.Mock((*mockCipher).GetDecryptor).To(func(_ *mockCipher, _, _ int64, _ []byte) (hook.Decryptor, error) {
		cancel()
		return &mockDecryptor{}, nil
	}).Build()
	defer getDecryptor.UnPatch()
	decrypt := mockey.Mock((*mockDecryptor).Decrypt).Return([]byte("plaintext"), nil).Build()
	defer decrypt.UnPatch()
	_, err := decryptPayloadWithRetry(ctx, 1, 10, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, decrypt.Times())
	_, err = decryptPayloadWithRetry(ctx, 1, 10, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, getDecryptor.Times(), "an already canceled caller must not invoke the plugin")
}

func TestCipherRetryClassification(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"nil", nil, false},
		{"unknown", errors.New("cipher: message authentication failed"), false},
		{"canceled", context.Canceled, false},
		{"permanent milvus", merr.ErrDataIntegrity, false},
		{"permanent outer error", merr.WrapErrServiceInternalErr(context.DeadlineExceeded, "permanent"), false},
		{"grpc invalid", status.Error(codes.InvalidArgument, "invalid ciphertext"), false},
		{"grpc denied", status.Error(codes.PermissionDenied, "access denied"), false},
		{"kms", errors.Wrap(ErrKmsKeyInvalid, "key unavailable"), true},
		{"milvus unavailable", errors.Wrap(merr.ErrServiceUnavailable, "KMS"), true},
		{"deadline", context.DeadlineExceeded, true},
		{"grpc unavailable", errors.Wrap(status.Error(codes.Unavailable, "KMS"), "plugin"), true},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "KMS"), true},
		{"grpc throttle", status.Error(codes.ResourceExhausted, "KMS"), true},
		{"connection reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		{"connection refused", syscall.ECONNREFUSED, true},
		{"broken pipe", syscall.EPIPE, true},
		{"network timeout", &net.DNSError{IsTimeout: true}, true},
		{"permanent dns", &net.DNSError{IsNotFound: true}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.retry, isRetryableCipherError(test.err))
		})
	}
}

func TestDecodePayloadRetryBoundaries(t *testing.T) {
	getCipher := mockey.Mock(getCipher).Return(&mockCipher{}, nil).Build()
	defer getCipher.UnPatch()
	getDecryptor := mockey.Mock((*mockCipher).GetDecryptor).Return(&mockDecryptor{}, nil).Build()
	defer getDecryptor.UnPatch()
	decrypt := mockey.Mock((*mockDecryptor).Decrypt).Return([]byte{0xff}, nil).Build()
	defer decrypt.UnPatch()

	plain := &messageImpl{payload: []byte("plaintext")}
	//nolint:staticcheck // Preserve the existing nil-context compatibility.
	payload, err := plain.decodePayload(nil)
	require.NoError(t, err)
	require.Equal(t, plain.payload, payload)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = plain.decodePayload(ctx)
	require.ErrorIs(t, err, context.Canceled)

	malformed := &messageImpl{properties: propertiesImpl{messageCipherHeader: "invalid base64!"}}
	_, err = malformed.decodePayload(context.Background())
	require.Error(t, err)
	require.Zero(t, getCipher.Times(), "plain, canceled and malformed-header messages must not call the cipher")

	header, err := EncodeProto(&messagespb.CipherHeader{EzId: 1, CollectionId: 10})
	require.NoError(t, err)
	encrypted := &messageImpl{payload: []byte("ciphertext"), properties: propertiesImpl{messageCipherHeader: header}}
	body, err := decodeProtoB[*msgpb.InsertRequest](context.Background(), encrypted)
	require.ErrorIs(t, err, ErrMalformedBody)
	require.Nil(t, body)
	require.Equal(t, 1, getDecryptor.Times())
	require.Equal(t, 1, decrypt.Times(), "malformed protobuf after successful decryption must not retry")
}
