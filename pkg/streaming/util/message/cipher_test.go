package message

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
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

func TestGetDecryptorWithRetry_Success(t *testing.T) {
	// Setup: register a mock cipher that succeeds immediately
	origCipher := cipher
	defer func() { cipher = origCipher }()

	mockDec := &mockDecryptor{}
	cipher = &mockCipher{
		getDecryptorFunc: func(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
			return mockDec, nil
		},
	}

	// Test
	decryptor, err := getDecryptorWithRetry(123, 456, []byte("safe-key"))

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, mockDec, decryptor)
}

func TestGetDecryptorWithRetry_NonRetriableError(t *testing.T) {
	// Setup: register a mock cipher that returns a non-retriable error
	origCipher := cipher
	defer func() { cipher = origCipher }()

	nonRetriableErr := errors.New("non-retriable error")
	cipher = &mockCipher{
		getDecryptorFunc: func(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
			return nil, nonRetriableErr
		},
	}

	// Test
	start := time.Now()
	decryptor, err := getDecryptorWithRetry(123, 456, []byte("safe-key"))
	duration := time.Since(start)

	// Assert - should fail immediately without retry
	assert.Error(t, err)
	assert.Nil(t, decryptor)
	assert.Contains(t, err.Error(), "non-retriable error")
	assert.Less(t, duration.Milliseconds(), int64(50), "should return quickly without retry")
}

func TestGetDecryptorWithRetry_KmsKeyInvalidWithRetry(t *testing.T) {
	// Setup: register a mock cipher that fails 3 times with KmsKeyInvalid, then succeeds
	origCipher := cipher
	defer func() { cipher = origCipher }()

	var attemptCount int32
	mockDec := &mockDecryptor{}
	cipher = &mockCipher{
		getDecryptorFunc: func(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
			attempt := atomic.AddInt32(&attemptCount, 1)
			if attempt < 4 {
				return nil, fmt.Errorf("%w: ezID=%d, state=revoked", ErrKmsKeyInvalid, ezID)
			}
			return mockDec, nil
		},
	}

	// Test
	start := time.Now()
	decryptor, err := getDecryptorWithRetry(123, 456, []byte("safe-key"))
	duration := time.Since(start)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, mockDec, decryptor)
	assert.Equal(t, int32(4), atomic.LoadInt32(&attemptCount))
	// Should have retried at least 3 times with exponential backoff
	// Min expected duration: 100ms + 200ms + 400ms = 700ms
	assert.Greater(t, duration.Milliseconds(), int64(300), "should have retried with delay")
}

func TestGetDecryptorWithRetry_ExponentialBackoff(t *testing.T) {
	// Setup: register a mock cipher that tracks timing between attempts
	origCipher := cipher
	defer func() { cipher = origCipher }()

	var attemptCount int32
	var lastAttemptTime time.Time
	var backoffs []time.Duration
	mockDec := &mockDecryptor{}

	cipher = &mockCipher{
		getDecryptorFunc: func(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
			attempt := atomic.AddInt32(&attemptCount, 1)
			now := time.Now()

			if !lastAttemptTime.IsZero() && attempt > 1 {
				backoff := now.Sub(lastAttemptTime)
				backoffs = append(backoffs, backoff)
			}
			lastAttemptTime = now

			if attempt < 5 {
				return nil, ErrKmsKeyInvalid
			}
			return mockDec, nil
		},
	}

	// Test
	_, err := getDecryptorWithRetry(123, 456, []byte("safe-key"))

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, int32(5), atomic.LoadInt32(&attemptCount))
	assert.Len(t, backoffs, 4)

	// Verify exponential backoff (with some tolerance for timing variance)
	// Expected: ~100ms, ~200ms, ~400ms, ~800ms
	expectedBackoffs := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
	}

	for i, backoff := range backoffs {
		expected := expectedBackoffs[i]
		// Allow 50% tolerance due to timing variance
		minBackoff := time.Duration(float64(expected) * 0.5)
		maxBackoff := time.Duration(float64(expected) * 1.5)
		assert.GreaterOrEqual(t, backoff, minBackoff,
			"backoff %d should be at least %v, got %v", i, minBackoff, backoff)
		assert.LessOrEqual(t, backoff, maxBackoff,
			"backoff %d should be at most %v, got %v", i, maxBackoff, backoff)
	}
}

func TestGetDecryptorWithRetry_MaxBackoffCap(t *testing.T) {
	// Setup: register a mock cipher that fails many times to test max backoff
	origCipher := cipher
	defer func() { cipher = origCipher }()

	var attemptCount int32
	var lastAttemptTime time.Time
	var backoffs []time.Duration
	mockDec := &mockDecryptor{}

	cipher = &mockCipher{
		getDecryptorFunc: func(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
			attempt := atomic.AddInt32(&attemptCount, 1)
			now := time.Now()

			if !lastAttemptTime.IsZero() {
				backoff := now.Sub(lastAttemptTime)
				backoffs = append(backoffs, backoff)
			}
			lastAttemptTime = now

			// Succeed after enough retries to test max backoff
			if attempt < 10 {
				return nil, ErrKmsKeyInvalid
			}
			return mockDec, nil
		},
	}

	// Test
	_, err := getDecryptorWithRetry(123, 456, []byte("safe-key"))

	// Assert
	assert.NoError(t, err)

	// Verify that backoff is capped at 3 seconds
	// After several retries, all backoffs should be at max (3s)
	if len(backoffs) > 5 {
		for i := 5; i < len(backoffs); i++ {
			// Allow some tolerance
			assert.LessOrEqual(t, backoffs[i], 3500*time.Millisecond,
				"backoff %d should be capped at ~3s, got %v", i, backoffs[i])
			assert.GreaterOrEqual(t, backoffs[i], 2500*time.Millisecond,
				"backoff %d should be around 3s, got %v", i, backoffs[i])
		}
	}
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
