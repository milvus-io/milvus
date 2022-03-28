package compressor

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

type CompressType string

const (
	CompressTypeZstd CompressType = "zstd"

	DefaultCompressAlgorithm CompressType = CompressTypeZstd
)

type Compressor interface {
	Compress(in io.Reader) error
	CompressBytes(src, dst []byte) []byte
	ResetWriter(out io.Writer)
	// Flush() error
	Close() error
	GetType() CompressType
}

type Decompressor interface {
	Decompress(out io.Writer) error
	DecompressBytes(src, dst []byte) ([]byte, error)
	ResetReader(in io.Reader)
	Close()
	GetType() CompressType
}

var (
	_ Compressor   = (*ZstdCompressor)(nil)
	_ Decompressor = (*ZstdDecompressor)(nil)
)

type ZstdCompressor struct {
	encoder *zstd.Encoder
}

// For compressing small blocks, pass nil to the `out` parameter
func NewZstdCompressor(out io.Writer, opts ...zstd.EOption) (*ZstdCompressor, error) {
	encoder, err := zstd.NewWriter(out, opts...)
	if err != nil {
		return nil, err
	}

	return &ZstdCompressor{encoder}, nil
}

// Use case: compress stream
// Call Close() to make sure the data is flushed to the underlying writer
// after the last Compress() call
func (c *ZstdCompressor) Compress(in io.Reader) error {
	_, err := io.Copy(c.encoder, in)
	if err != nil {
		c.encoder.Close()
		return err
	}

	return nil
}

// Use case: compress small blocks
// This compresses the src bytes and appends it to the dst bytes, then return the result
// This can be called concurrently
func (c *ZstdCompressor) CompressBytes(src []byte, dst []byte) []byte {
	return c.encoder.EncodeAll(src, dst)
}

// Reset the writer to reuse the compressor
func (c *ZstdCompressor) ResetWriter(out io.Writer) {
	c.encoder.Reset(out)
}

// The Flush() seems to not work as expected, remove it for now
// Replace it with Close()
// func (c *ZstdCompressor) Flush() error {
// 	if c.encoder != nil {
// 		return c.encoder.Flush()
// 	}

// 	return nil
// }

// The compressor is still re-used after calling this
func (c *ZstdCompressor) Close() error {
	return c.encoder.Close()
}

func (c *ZstdCompressor) GetType() CompressType {
	return CompressTypeZstd
}

type ZstdDecompressor struct {
	decoder *zstd.Decoder
}

// For compressing small blocks, pass nil to the `in` parameter
func NewZstdDecompressor(in io.Reader, opts ...zstd.DOption) (*ZstdDecompressor, error) {
	decoder, err := zstd.NewReader(in, opts...)
	if err != nil {
		return nil, err
	}

	return &ZstdDecompressor{decoder}, nil
}

// Usa case: decompress stream
// Write the decompressed data into `out`
func (dec *ZstdDecompressor) Decompress(out io.Writer) error {
	_, err := io.Copy(out, dec.decoder)
	if err != nil {
		dec.decoder.Close()
		return err
	}

	return nil
}

// Use case: decompress small blocks
// This decompresses the src bytes and appends it to the dst bytes, then return the result
// This can be called concurrently
func (dec *ZstdDecompressor) DecompressBytes(src []byte, dst []byte) ([]byte, error) {
	return dec.decoder.DecodeAll(src, dst)
}

// Reset the reader to reuse the decompressor
func (dec *ZstdDecompressor) ResetReader(in io.Reader) {
	dec.decoder.Reset(in)
}

// NOTICE: not like compressor, the decompressor is not usable after calling this
func (dec *ZstdDecompressor) Close() {
	dec.decoder.Close()
}

func (dec *ZstdDecompressor) GetType() CompressType {
	return CompressTypeZstd
}

// Global methods

// Usa case: compress stream, large object only once
// This can be called concurrently
// Try ZstdCompressor for better efficiency if you need compress mutiple streams one by one
func ZstdCompress(in io.Reader, out io.Writer, opts ...zstd.EOption) error {
	enc, err := NewZstdCompressor(out, opts...)
	if err != nil {
		return err
	}

	if err = enc.Compress(in); err != nil {
		enc.Close()
		return err
	}

	return enc.Close()
}

// Use case: decompress stream, large object only once
// This can be called concurrently
// Try ZstdDecompressor for better efficiency if you need decompress mutiple streams one by one
func ZstdDecompress(in io.Reader, out io.Writer, opts ...zstd.DOption) error {
	dec, err := NewZstdDecompressor(in, opts...)
	if err != nil {
		return err
	}
	defer dec.Close()

	if err = dec.Decompress(out); err != nil {
		return err
	}

	return nil
}

var (
	globalZstdCompressor, _   = zstd.NewWriter(nil)
	globalZstdDecompressor, _ = zstd.NewReader(nil)
)

// Use case: compress small blocks
// This can be called concurrently
func ZstdCompressBytes(src, dst []byte) []byte {
	return globalZstdCompressor.EncodeAll(src, dst)
}

// Use case: decompress small blocks
// This can be called concurrently
func ZstdDecompressBytes(src, dst []byte) ([]byte, error) {
	return globalZstdDecompressor.DecodeAll(src, dst)
}
