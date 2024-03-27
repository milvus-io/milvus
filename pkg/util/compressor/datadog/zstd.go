package datadog

import (
	"bytes"
	"io"

	"github.com/DataDog/zstd" // Import Datadog's zstd package
	"google.golang.org/grpc/encoding"
)

const Name = "datadog"

type grpcCompressor struct{}

func init() {
	c := &grpcCompressor{}
	encoding.RegisterCompressor(c)
}

func (c *grpcCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &zstdWriteCloser{
		writer: w,
	}, nil
}

type zstdWriteCloser struct {
	writer io.Writer    // Compressed data will be written here.
	buf    bytes.Buffer // Buffer uncompressed data here, compress on Close.
}

func (z *zstdWriteCloser) Write(p []byte) (int, error) {
	return z.buf.Write(p)
}

func (z *zstdWriteCloser) Close() error {
	// prefer faster compression decompression rather than compression ratio
	compressed, err := zstd.CompressLevel(nil, z.buf.Bytes(), 3)
	if err != nil {
		return err
	}
	_, err = z.writer.Write(compressed)
	return err
}

func (c *grpcCompressor) Decompress(r io.Reader) (io.Reader, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// Use Datadog's API to decompress data
	uncompressed, err := zstd.Decompress(nil, compressed)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(uncompressed), nil
}

func (c *grpcCompressor) Name() string {
	return Name
}
