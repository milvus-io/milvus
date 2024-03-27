package deflate

import (
	"compress/zlib"
	"io"
	"sync"

	"google.golang.org/grpc/encoding"
)

// Name is the name registered for the DEFLATE compressor.
// https://www.rfc-editor.org/rfc/rfc1950
const Name = "deflate"

func init() {
	c := &compressor{}
	c.poolCompressor.New = func() interface{} {
		return &writer{Writer: zlib.NewWriter(nil), pool: &c.poolCompressor}
	}
	encoding.RegisterCompressor(c)
}

type writer struct {
	*zlib.Writer
	pool *sync.Pool
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	dw := c.poolCompressor.Get().(*writer)
	dw.Reset(w)
	return dw, nil
}

func (w *writer) Close() error {
	defer w.pool.Put(w)
	return w.Writer.Close()
}

type reader struct {
	io.ReadCloser
	pool *sync.Pool
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	dr, inPool := c.poolDecompressor.Get().(*reader)
	if !inPool {
		zlibReader, _ := zlib.NewReader(r)
		return &reader{ReadCloser: zlibReader, pool: &c.poolDecompressor}, nil
	}
	if err := dr.ReadCloser.(zlib.Resetter).Reset(r, nil); err != nil {
		c.poolDecompressor.Put(dr)
		return nil, err
	}
	return dr, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	if err == io.EOF {
		r.pool.Put(r)
	}
	return n, err
}

func (c *compressor) Name() string {
	return Name
}

type compressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}
