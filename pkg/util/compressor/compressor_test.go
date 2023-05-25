package compressor

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZstdCompress(t *testing.T) {
	data := "hello zstd algorithm!"
	compressed := new(bytes.Buffer)
	origin := new(bytes.Buffer)

	enc, err := NewZstdCompressor(compressed)
	assert.NoError(t, err)
	testCompress(t, data, enc, compressed, origin)

	// Reuse test
	compressed.Reset()
	origin.Reset()

	enc.ResetWriter(compressed)

	testCompress(t, data+": reuse", enc, compressed, origin)

	// Test type
	dec, err := NewZstdDecompressor(nil)
	assert.NoError(t, err)
	assert.Equal(t, enc.GetType(), CompressTypeZstd)
	assert.Equal(t, dec.GetType(), CompressTypeZstd)
}

func testCompress(t *testing.T, data string, enc Compressor, compressed, origin *bytes.Buffer) {
	compressedBytes := make([]byte, 0)
	originBytes := make([]byte, 0)

	err := enc.Compress(strings.NewReader(data))
	assert.NoError(t, err)
	err = enc.Close()
	assert.NoError(t, err)
	compressedBytes = enc.CompressBytes([]byte(data), compressedBytes)
	assert.Equal(t, compressed.Bytes(), compressedBytes)

	// Close() method should satisfy idempotence
	err = enc.Close()
	assert.NoError(t, err)

	dec, err := NewZstdDecompressor(compressed)
	assert.NoError(t, err)
	err = dec.Decompress(origin)
	assert.NoError(t, err)
	originBytes, err = dec.DecompressBytes(compressedBytes, originBytes)
	assert.NoError(t, err)
	assert.Equal(t, origin.Bytes(), originBytes)

	assert.Equal(t, data, origin.String())

	// Mock error reader/writer
	errReader := &ErrReader{Err: io.ErrUnexpectedEOF}
	errWriter := &ErrWriter{Err: io.ErrShortWrite}

	err = enc.Compress(errReader)
	assert.ErrorIs(t, err, errReader.Err)

	dec.ResetReader(bytes.NewReader(compressed.Bytes()))
	err = dec.Decompress(errWriter)
	assert.ErrorIs(t, err, errWriter.Err)

	// Use closed decompressor
	dec.ResetReader(bytes.NewReader(compressed.Bytes()))
	dec.Close()
	err = dec.Decompress(origin)
	assert.Error(t, err)
}

type ErrReader struct {
	Err error
}

func (r *ErrReader) Read(p []byte) (n int, err error) {
	return 0, r.Err
}

type ErrWriter struct {
	Err error
}

func (w *ErrWriter) Write(p []byte) (n int, err error) {
	return 0, w.Err
}
