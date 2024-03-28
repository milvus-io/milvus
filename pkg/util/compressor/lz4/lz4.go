package lz4

import (
	"io"
	"sync"

	"github.com/pierrec/lz4"
	"google.golang.org/grpc/encoding"
)

// Name is the name registered for the Lz4 compressor.
const Name = "lz4"

func init() {
	c := &compressor{}
	c.poolCompressor.New = func() interface{} {
		return &writer{Writer: lz4.NewWriter(io.Discard), pool: &c.poolCompressor}
	}
	encoding.RegisterCompressor(c)
}

type writer struct {
	*lz4.Writer
	pool *sync.Pool
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	z := c.poolCompressor.Get().(*writer)
	z.Writer.Reset(w)
	return z, nil
}

func (z *writer) Close() (err error) {
	err = z.Writer.Close()
	z.pool.Put(z)
	return
}

type reader struct {
	*lz4.Reader
	pool *sync.Pool
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	z, inPool := c.poolDecompressor.Get().(*reader)
	if inPool {
		z.Reset(r)
		return z, nil
	}
	return &reader{Reader: lz4.NewReader(r), pool: &c.poolDecompressor}, nil
}

func (z *reader) Read(p []byte) (n int, err error) {
	if n, err = z.Reader.Read(p); err == io.EOF {
		z.pool.Put(z)
	}
	return
}

func (c *compressor) Name() string {
	return Name
}

type compressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}
