// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package httpcompress negotiates HTTP body compression for the RESTful API.
//
// Both directions follow plain HTTP content coding, so a client that sends
// neither Accept-Encoding nor Content-Encoding is served exactly as before:
//
//   - Responses are compressed only when proxy.http.compression.enabled is on
//     and the request's Accept-Encoding allows zstd or gzip; the response says
//     so in Content-Encoding.
//   - A request body is decompressed whenever it carries Content-Encoding zstd
//     or gzip, regardless of that switch: the client chose to compress, so
//     whether it is accepted must not depend on one cluster's configuration.
//     A registered coding this server cannot decode is rejected with 415 and
//     the supported list, per RFC 7694, rather than being parsed as JSON and
//     reported as a malformed body. A plain body labeled gzip or zstd, which
//     the server used to parse as JSON, is rejected too, with 400. Decoding is
//     a gin middleware installed after authentication, so a request that
//     fails authentication, or that matches no route, never makes the proxy
//     expand a body.
//   - Every response advertises Accept-Encoding: zstd, gzip (RFC 7694 §3), which
//     tells a client that the instance that answered accepts compressed request
//     bodies. A server without this feature never sends it. The header speaks
//     for one instance only: behind a load balancer the next request can reach
//     an older one, which answers a compressed body with 200 and code 1801,
//     without this header, before running anything. A client keeps a fallback
//     that resends the body uncompressed on such an answer.
//
// Response compression sits outside gin, so gin's byte counters see the
// uncompressed response. The decoded request sets Content-Length to its
// decoded size before the per-route payload metrics read it. Both match the
// gRPC stats handler, which records InPayload.Length rather than the wire length.
package httpcompress

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/net/http2"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	ContentEncodingZstd = "zstd"
	ContentEncodingGzip = "gzip"

	contentEncodingXGzip = "x-gzip"

	// SupportedRequestEncodings is advertised in the Accept-Encoding response
	// header, most preferred first.
	SupportedRequestEncodings = ContentEncodingZstd + ", " + ContentEncodingGzip

	headerAcceptEncoding  = "Accept-Encoding"
	headerContentEncoding = "Content-Encoding"
	headerContentLength   = "Content-Length"
	headerVary            = "Vary"

	// Field names of the RESTful error body, as internal/http's HTTPReturnCode
	// and HTTPReturnMessage. That package would pull about 190 packages into
	// this one for two strings.
	returnCode    = "code"
	returnMessage = "message"

	// compressionZstdWindow bounds the history a pooled zstd encoder keeps.
	// Measured on 449 KiB text and 840 KiB vector query results, 512 KiB
	// compresses within 0.1% of 1 MiB and retains 1.8 MB per encoder instead
	// of 2.9 MB; going lower saves nothing more, since the block buffers and
	// hash tables dominate from there.
	compressionZstdWindow = 512 << 10
	// compressionZstdMaxDecodeWindow bounds the window a client's frame may
	// declare. The decoder allocates its history from the frame header before
	// producing any output, so the decoded-size limit alone does not bound it.
	compressionZstdMaxDecodeWindow = 8 << 20
)

// A zero status tells the middleware to abort the response after logging cancellation.
type requestDecodeError struct {
	status int
	err    error
}

func unsupportedEncoding(format string, args ...any) *requestDecodeError {
	return &requestDecodeError{
		status: http.StatusUnsupportedMediaType,
		err: merr.WrapErrParameterInvalidMsg("unsupported Content-Encoding %s, supported: %s",
			fmt.Sprintf(format, args...), SupportedRequestEncodings),
	}
}

// compressSlots caps how many responses hold an encoder at once. An encoder
// is held for the whole response, including while a slow client drains it, so
// without a cap the pooled encoders' memory grows with the number of open
// responses. Clients that allow identity can fall back to an uncompressed response.
var compressSlots = make(chan struct{}, 8*runtime.GOMAXPROCS(0))

// Handler negotiates HTTP body compression in both directions. It
// must wrap only the RESTful handler: in shared-port mode gRPC requests are
// dispatched before it, and gRPC does its own message compression.
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := &paramtable.Get().HTTPCfg
		// RequestBodyMiddleware decodes request bodies whatever the switch says,
		// so every response advertises it.
		w.Header().Set(headerAcceptEncoding, SupportedRequestEncodings)
		w.Header().Set(headerVary, headerAcceptEncoding)
		// Milvus serves bodyless HEAD and CORS preflight responses.
		if r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		encoding := negotiateResponseEncoding(r.Header.Values(headerAcceptEncoding))
		if encoding.coding == "" || !cfg.CompressionEnabled.GetAsBool() {
			if !encoding.identityAllowed {
				w.WriteHeader(http.StatusNotAcceptable)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		cw := newCompressResponseWriter(w, encoding.coding, cfg.CompressionMinResponseSize.GetAsInt())
		if !encoding.identityAllowed {
			// Reserve before dispatch: a failed negotiation must never hide an
			// insert or another operation the handler has already performed.
			if !cw.acquireSlot() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			cw.minSize = 0
		}
		// release runs on every exit, including the panic a handler raises to
		// abort the response; finish only runs on a normal return, so an aborted
		// response is never given a valid compressed trailer.
		defer cw.release()
		next.ServeHTTP(cw, r)
		if err := cw.finish(); err != nil {
			mlog.Warn(r.Context(), "failed to finish compressed RESTful response",
				mlog.String("encoding", encoding.coding), mlog.Err(err))
		}
	})
}

// RequestBodyMiddleware decodes a request body sent with Content-Encoding
// zstd or gzip. It must run after authentication: before it, anyone could make
// the proxy expand a small body up to proxy.grpc.serverMaxRecvSize. A request that
// matches no route is left alone, since nothing will read its body.
//
// The body is decoded in full before the route handlers run, so an oversized
// or corrupt body is answered with 413 or 400 instead of a generic parse
// error, and Content-Length reports the decoded size that the payload metrics
// read. DQL admission runs before this middleware; the route's Request-Timeout
// clock starts after it. The decoded bytes are also cached under
// gin.BodyBytesKey, where ShouldBindBodyWith and the v2 insert binder look
// first, so they do not read the body out of the request a second time.
func RequestBodyMiddleware(abortResponse gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.FullPath() == "" {
			c.Next()
			return
		}
		// The bound a single gRPC request to this proxy has, so one setting governs
		// both. It is not an equivalent bound: JSON is several times larger than
		// the protobuf of the same rows, and a plain body has no bound beyond the
		// bytes the client sends.
		limit := paramtable.Get().ProxyGrpcServerCfg.ServerMaxRecvSize.GetAsInt64()
		decoded, rejected := decodeRequestBody(c.Request, limit)
		if rejected != nil {
			// Where the access log reads a RESTful request's result.
			c.Set(info.ContextReturnCode, merr.Code(rejected.err))
			c.Set(info.ContextReturnMessage, rejected.err.Error())
			if rejected.status == 0 {
				abortResponse(c)
				return
			}
			c.AbortWithStatusJSON(rejected.status, gin.H{
				returnCode:    merr.Code(rejected.err),
				returnMessage: rejected.err.Error(),
			})
			return
		}
		if decoded != nil {
			// Handed over for good: handlers keep references into this buffer (row
			// conversion borrows it through unsafe.String), so it must never be
			// pooled or reused.
			c.Set(gin.BodyBytesKey, decoded)
		}
		c.Next()
	}
}

func requestContentCoding(values []string) (string, *requestDecodeError) {
	var codings []string
	for _, value := range values {
		for token := range strings.SplitSeq(value, ",") {
			token = strings.ToLower(strings.TrimSpace(token))
			if _, registered := registeredContentCodings[token]; registered {
				codings = append(codings, token)
			}
		}
	}
	if len(codings) == 0 {
		// Nothing a body can be coded with: no header, identity, or a value
		// that is not a content coding at all, such as a charset sent as
		// "Content-Encoding: UTF-8". The server ignored the header before this
		// layer existed, so it keeps ignoring it and leaves the request alone.
		return "", nil
	}
	if len(codings) > 1 {
		return "", unsupportedEncoding("%q (stacked codings)", strings.Join(codings, ", "))
	}
	coding := codings[0]
	if coding == contentEncodingXGzip {
		coding = ContentEncodingGzip
	}
	if coding != ContentEncodingZstd && coding != ContentEncodingGzip {
		return "", unsupportedEncoding("%q", coding)
	}
	return coding, nil
}

// decodeRequestBody replaces a compressed request body with its decoded bytes
// and returns them. It returns nil bytes when the body is not compressed.
func decodeRequestBody(r *http.Request, limit int64) ([]byte, *requestDecodeError) {
	coding, rejected := requestContentCoding(r.Header.Values(headerContentEncoding))
	if coding == "" {
		return nil, rejected
	}
	upload := &uploadReader{Reader: r.Body}
	// The pooled gzip decoder keeps its input reader until its next Reset.
	defer func() { *upload = uploadReader{} }()
	decoded, err := decompress(coding, upload, limit)
	if err == nil && int64(len(decoded)) > limit {
		return nil, &requestDecodeError{
			status: http.StatusRequestEntityTooLarge,
			err: merr.WrapErrParameterTooLarge("request body",
				fmt.Sprintf("the %s body expands past %d bytes (proxy.grpc.serverMaxRecvSize)", coding, limit)),
		}
	}
	readErr, ctxErr := upload.err, r.Context().Err()
	if readErr != nil || ctxErr != nil {
		var netErr net.Error
		isNetErr := errors.As(readErr, &netErr)
		var streamErr http2.StreamError
		isStreamErr := errors.As(readErr, &streamErr)
		// A socket failure or stream reset can also cancel the request context.
		switch {
		case isNetErr && netErr.Timeout(), errors.Is(readErr, context.DeadlineExceeded), ctxErr == context.DeadlineExceeded:
			return nil, &requestDecodeError{status: http.StatusRequestTimeout, err: context.DeadlineExceeded}
		case errors.Is(readErr, context.Canceled), isStreamErr && streamErr.Code == http2.ErrCodeCancel:
			return nil, &requestDecodeError{err: context.Canceled}
		case isNetErr, isStreamErr, errors.Is(readErr, io.ErrUnexpectedEOF), errors.Is(readErr, io.ErrClosedPipe):
			return nil, &requestDecodeError{
				status: http.StatusServiceUnavailable,
				err:    merr.WrapErrServiceUnavailableErr(readErr, "reading compressed request body"),
			}
		case ctxErr == context.Canceled:
			return nil, &requestDecodeError{err: context.Canceled}
		default:
			err = readErr
		}
	}
	if err != nil {
		return nil, &requestDecodeError{
			status: http.StatusBadRequest,
			err:    merr.WrapErrParameterInvalidErr(err, "invalid %s request body", coding),
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(decoded))
	r.ContentLength = int64(len(decoded))
	r.Header.Set(headerContentLength, strconv.Itoa(len(decoded)))
	r.Header.Del(headerContentEncoding)
	return decoded, nil
}

// Keep the source error even when a codec converts a short read to EOF.
type uploadReader struct {
	io.Reader
	err error
}

func (u *uploadReader) Read(p []byte) (int, error) {
	n, err := u.Reader.Read(p)
	if err != nil && err != io.EOF && u.err == nil {
		u.err = err
	}
	return n, err
}

// decompress reads up to limit+1 decoded bytes from a gzip or zstd body.
func decompress(coding string, body io.Reader, limit int64) ([]byte, error) {
	var reader io.Reader
	if coding == ContentEncodingZstd {
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		defer func() {
			_ = dec.Reset(nil)
			zstdDecoderPool.Put(dec)
		}()
		if err := dec.Reset(body); err != nil {
			return nil, err
		}
		reader = dec
	} else {
		dec := gzipDecoderPool.Get().(*gzip.Reader)
		defer gzipDecoderPool.Put(dec)
		if err := dec.Reset(body); err != nil {
			return nil, err
		}
		reader = dec
	}

	// Read one byte past the limit so an exactly-full body is accepted and an
	// over-full one is told apart from it without decoding the rest. There is
	// no byte past math.MaxInt64, which means no limit, as it does for gRPC.
	readLimit := limit
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	// io.ReadAll returns a slice of exactly the decoded size. A growing
	// bytes.Buffer could keep up to as much again as slack, and the body stays
	// referenced for the whole request.
	return io.ReadAll(io.LimitReader(reader, readLimit))
}

type responseEncoding struct {
	coding          string // empty when no response codec is selected
	identityAllowed bool
}

// negotiateResponseEncoding applies RFC 9110 §12.5.3. Unlisted identity is
// an available fallback unless excluded by *;q=0. Only an explicit identity
// weight expresses a preference over a particular coding.
func negotiateResponseEncoding(values []string) responseEncoding {
	if len(values) == 0 {
		return responseEncoding{identityAllowed: true}
	}
	weights := map[string]float64{}
	for _, value := range values {
		for token := range strings.SplitSeq(value, ",") {
			name, q, ok := parseAcceptEncodingToken(token)
			if !ok {
				continue
			}
			switch name {
			case contentEncodingXGzip:
				weights[ContentEncodingGzip] = q
			case ContentEncodingGzip, ContentEncodingZstd, "identity", "*":
				weights[name] = q
			}
		}
	}
	best, bestQ := "", 0.0
	for _, coding := range []string{ContentEncodingZstd, ContentEncodingGzip} {
		q, listed := weights[coding]
		if !listed {
			q, listed = weights["*"]
		}
		if listed && q > bestQ {
			best, bestQ = coding, q
		}
	}
	identityQ, explicit := weights["identity"]
	if explicit {
		if identityQ > bestQ {
			best = ""
		}
		return responseEncoding{coding: best, identityAllowed: identityQ > 0}
	}
	wildcardQ, wildcard := weights["*"]
	return responseEncoding{coding: best, identityAllowed: !wildcard || wildcardQ > 0}
}

func parseAcceptEncodingToken(token string) (string, float64, bool) {
	name, params, _ := strings.Cut(token, ";")
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "", 0, false
	}
	q := 1.0
	for param := range strings.SplitSeq(params, ";") {
		key, value, found := strings.Cut(strings.TrimSpace(param), "=")
		if !found || !strings.EqualFold(strings.TrimSpace(key), "q") {
			continue
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || math.IsNaN(parsed) || parsed < 0 || parsed > 1 {
			return "", 0, false
		}
		q = parsed
	}
	return name, q, true
}

// streamEncoder is the part of *zstd.Encoder and *gzip.Writer the response
// writer uses.
type streamEncoder interface {
	io.WriteCloser
	Flush() error
	Reset(w io.Writer)
}

// registeredContentCodings is the IANA HTTP Content Coding Registry without
// identity, which means no coding. Only a registered coding says the body is
// transformed; a coding this server cannot decode is rejected with 415, and
// any other value is ignored as it was before.
var registeredContentCodings = map[string]struct{}{
	"aes128gcm": {}, "br": {}, "compress": {}, "dcb": {}, "dcz": {}, "deflate": {}, "exi": {},
	ContentEncodingGzip: {}, "pack200-gzip": {}, "x-compress": {},
	contentEncodingXGzip: {}, ContentEncodingZstd: {},
}

var (
	zstdEncoderPool = sync.Pool{New: func() any {
		// Concurrency 1 keeps the streaming encoder on the caller's goroutine:
		// it starts no goroutines, so an encoder the pool drops needs no Close.
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithWindowSize(compressionZstdWindow),
		)
		if err != nil {
			panic(fmt.Sprintf("invalid static zstd encoder options: %v", err))
		}
		return enc
	}}
	gzipEncoderPool = sync.Pool{New: func() any {
		enc, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		if err != nil {
			panic(fmt.Sprintf("invalid static gzip level: %v", err))
		}
		return enc
	}}
	zstdDecoderPool = sync.Pool{New: func() any {
		// Concurrency 1 for the same reason as the encoder: above 1 a decoder
		// holds goroutines that only Close releases.
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxWindow(compressionZstdMaxDecodeWindow),
		)
		if err != nil {
			panic(fmt.Sprintf("invalid static zstd decoder options: %v", err))
		}
		return dec
	}}
	gzipDecoderPool = sync.Pool{New: func() any { return new(gzip.Reader) }}
)

func getEncoder(coding string, dst io.Writer) streamEncoder {
	var enc streamEncoder
	if coding == ContentEncodingZstd {
		enc = zstdEncoderPool.Get().(*zstd.Encoder)
	} else {
		enc = gzipEncoderPool.Get().(*gzip.Writer)
	}
	enc.Reset(dst)
	return enc
}

func putEncoder(coding string, enc streamEncoder) {
	// Drop the reference to the response writer before parking the encoder.
	enc.Reset(io.Discard)
	if coding == ContentEncodingZstd {
		zstdEncoderPool.Put(enc)
	} else {
		gzipEncoderPool.Put(enc)
	}
}

// compressResponseWriter holds back the status and the first minSize bytes so
// it can decide whether compressing is worth it, then either streams the rest
// through an encoder or passes it through unchanged. The hold-back is outside
// the encoder slots, so proxy.http.compression.minResponseSize should stay
// small.
type compressResponseWriter struct {
	http.ResponseWriter
	coding  string
	minSize int

	status   int
	pending  []byte
	decided  bool
	slotHeld bool
	enc      streamEncoder
}

func newCompressResponseWriter(w http.ResponseWriter, coding string, minSize int) *compressResponseWriter {
	return &compressResponseWriter{ResponseWriter: w, coding: coding, minSize: minSize}
}

func (w *compressResponseWriter) WriteHeader(code int) {
	if w.decided {
		// Matches net/http: a status after the header is sent is ignored.
		return
	}
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		// Informational responses go out immediately and do not end the header.
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.status == 0 {
		w.status = code
	}
}

func (w *compressResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if !w.decided {
		// A zero-length write only fixes the status, as in net/http: the
		// decision waits for bytes to measure and to sniff a type from.
		if len(p) == 0 || len(w.pending)+len(p) < w.minSize {
			w.pending = append(w.pending, p...)
			return len(p), nil
		}
		// The decision is known; write p straight through rather than copying
		// it. A buffered handler hands its whole body over in one Write.
		if err := w.decide(true, p); err != nil {
			return 0, err
		}
	}
	if w.enc != nil {
		return w.enc.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

// decide sends the header and the held-back bytes, compressing them when
// wanted, when the response is one that may carry a coded body, and when an
// encoder slot is free. next is the write that triggered the decision; it is
// only used to sniff a Content-Type and is written by the caller.
func (w *compressResponseWriter) decide(wantCompression bool, next []byte) error {
	w.decided = true
	if w.status == 0 {
		w.status = http.StatusOK
	}
	header := w.Header()
	if wantCompression && bodyAllowed(w.status) && header.Get(headerContentEncoding) == "" && w.acquireSlot() {
		if _, typed := header["Content-Type"]; !typed {
			// net/http does not sniff a body whose Content-Encoding is set. As
			// in net/http, a header flushed before any body byte gets no type.
			if sample := sniffSample(w.pending, next); len(sample) > 0 {
				header.Set("Content-Type", http.DetectContentType(sample))
			}
		}
		header.Set(headerContentEncoding, w.coding)
		header.Del(headerContentLength)
		w.enc = getEncoder(w.coding, w.ResponseWriter)
	}
	w.ResponseWriter.WriteHeader(w.status)
	pending := w.pending
	w.pending = nil
	if len(pending) == 0 {
		return nil
	}
	var err error
	if w.enc != nil {
		_, err = w.enc.Write(pending)
	} else {
		_, err = w.ResponseWriter.Write(pending)
	}
	return err
}

// Flush sends what has been written so far. A flush before minSize bytes are
// written still compresses: the handler asked for the bytes to go out now, and
// the rest of the body is likely to follow.
func (w *compressResponseWriter) Flush() {
	if !w.decided {
		if err := w.decide(true, nil); err != nil {
			return
		}
	}
	if w.enc != nil {
		if err := w.enc.Flush(); err != nil {
			return
		}
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// CloseNotify forwards to the writer underneath. gin's ResponseWriter asserts
// http.CloseNotifier without checking, and c.Stream calls it first, so without
// this a streaming handler panics whenever the client asked for compression.
// A writer underneath that cannot tell when the client goes away gets a
// channel that never fires, as the timeout recorder's does.
func (w *compressResponseWriter) CloseNotify() <-chan bool {
	if notifier, ok := w.ResponseWriter.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}
	return nil
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *compressResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// finish completes a response whose handler returned normally.
func (w *compressResponseWriter) finish() error {
	if !w.decided {
		if w.status == 0 && !w.slotHeld {
			// Nothing was written; let net/http send its implicit 200.
			return nil
		}
		// A reserved slot means identity was excluded, including for an
		// empty 200 response. Bodyless statuses are filtered in decide.
		if err := w.decide(w.slotHeld, nil); err != nil {
			return err
		}
	}
	if w.enc != nil {
		return w.enc.Close()
	}
	return nil
}

func (w *compressResponseWriter) release() {
	if w.enc != nil {
		putEncoder(w.coding, w.enc)
		w.enc = nil
	}
	if w.slotHeld {
		<-compressSlots
		w.slotHeld = false
	}
}

func (w *compressResponseWriter) acquireSlot() bool {
	if w.slotHeld {
		return true
	}
	select {
	case compressSlots <- struct{}{}:
		w.slotHeld = true
		return true
	default:
		return false
	}
}

// sniffSample is the first 512 bytes of the body, the most DetectContentType
// reads. next can be a whole buffered body, so no more than that is copied.
func sniffSample(pending, next []byte) []byte {
	const sniffLen = 512
	if len(pending) >= sniffLen {
		return pending[:sniffLen]
	}
	next = next[:min(len(next), sniffLen-len(pending))]
	if len(pending) == 0 {
		return next
	}
	return append(append(make([]byte, 0, sniffLen), pending...), next...)
}

func bodyAllowed(status int) bool {
	return status >= 200 && status != http.StatusNoContent && status != http.StatusResetContent && status != http.StatusNotModified
}
