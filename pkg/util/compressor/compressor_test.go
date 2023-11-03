package compressor

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/hardware"
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

func TestGlobalMethods(t *testing.T) {
	data := "hello zstd algorithm!"
	compressed := new(bytes.Buffer)
	compressedBytes := make([]byte, 0)
	origin := new(bytes.Buffer)
	originBytes := make([]byte, 0)

	err := ZstdCompress(strings.NewReader(data), compressed)
	assert.NoError(t, err)

	compressedBytes = ZstdCompressBytes([]byte(data), compressedBytes)
	assert.Equal(t, compressed.Bytes(), compressedBytes)

	err = ZstdDecompress(compressed, origin)
	assert.NoError(t, err)

	originBytes, err = ZstdDecompressBytes(compressedBytes, originBytes)
	assert.NoError(t, err)
	assert.Equal(t, origin.Bytes(), originBytes)

	assert.Equal(t, data, origin.String())

	// Mock error reader/writer
	errReader := &ErrReader{Err: io.ErrUnexpectedEOF}
	errWriter := &ErrWriter{Err: io.ErrShortWrite}

	compressedBytes = compressed.Bytes()
	compressed = bytes.NewBuffer(compressedBytes) // The old compressed buffer is closed
	err = ZstdCompress(errReader, compressed)
	assert.ErrorIs(t, err, errReader.Err)

	assert.Positive(t, len(compressedBytes))
	reader := bytes.NewReader(compressedBytes)
	err = ZstdDecompress(reader, errWriter)
	assert.ErrorIs(t, err, errWriter.Err)

	// Incorrect option
	err = ZstdCompress(strings.NewReader(data), compressed, zstd.WithWindowSize(3))
	assert.Error(t, err)
}

func TestCurrencyGlobalMethods(t *testing.T) {
	prefix := "Test Currency Global Methods"

	currency := hardware.GetCPUNum() * 2
	if currency < 6 {
		currency = 6
	}

	wg := sync.WaitGroup{}
	wg.Add(currency)
	for i := 0; i < currency; i++ {
		go func(idx int) {
			defer wg.Done()

			compressed := new(bytes.Buffer)
			compressedBytes := make([]byte, 0)
			origin := new(bytes.Buffer)
			originBytes := make([]byte, 0)

			data := prefix + fmt.Sprintf(": %d-th goroutine", idx)

			err := ZstdCompress(strings.NewReader(data), compressed)
			assert.NoError(t, err)
			compressedBytes = ZstdCompressBytes([]byte(data), compressedBytes)
			assert.Equal(t, compressed.Bytes(), compressedBytes)

			err = ZstdDecompress(compressed, origin)
			assert.NoError(t, err)
			originBytes, err = ZstdDecompressBytes(compressedBytes, originBytes)
			assert.NoError(t, err)
			assert.Equal(t, origin.Bytes(), originBytes)

			assert.Equal(t, data, origin.String())
		}(i)
	}
	wg.Wait()
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
