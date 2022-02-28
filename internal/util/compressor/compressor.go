package compressor

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

type CompressType int16

const (
	Zstd CompressType = iota + 1

	DefaultCompressAlgorithm CompressType = Zstd
)

type Compressor interface {
	Compress(in io.Reader) error
	ResetWriter(out io.Writer)
	// Flush() error
	Close() error
}

type Decompressor interface {
	Decompress(out io.Writer) error
	ResetReader(in io.Reader)
	Close()
}

var (
	_ Compressor   = (*ZstdCompressor)(nil)
	_ Decompressor = (*ZstdDecompressor)(nil)
)

type ZstdCompressor struct {
	encoder *zstd.Encoder
}

func NewZstdCompressor(out io.Writer, opts ...zstd.EOption) (*ZstdCompressor, error) {
	encoder, err := zstd.NewWriter(out, opts...)
	if err != nil {
		return nil, err
	}

	return &ZstdCompressor{encoder}, nil
}

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

type ZstdDecompressor struct {
	decoder *zstd.Decoder
}

func NewZstdDecompressor(in io.Reader, opts ...zstd.DOption) (*ZstdDecompressor, error) {
	decoder, err := zstd.NewReader(in, opts...)
	if err != nil {
		return nil, err
	}

	return &ZstdDecompressor{decoder}, nil
}

func (dec *ZstdDecompressor) Decompress(out io.Writer) error {
	_, err := io.Copy(out, dec.decoder)
	if err != nil {
		dec.decoder.Close()
		return err
	}

	return nil
}

func (dec *ZstdDecompressor) ResetReader(in io.Reader) {
	dec.decoder.Reset(in)
}

// NOTICE: not like compressor, the decompressor is not usable after calling this
func (dec *ZstdDecompressor) Close() {
	dec.decoder.Close()
}

// Global methods
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
