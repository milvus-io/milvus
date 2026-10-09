// Copyright 2026 The Milvus Authors. All rights reserved.
// Use of the upstream-derived transport is governed by the BSD-style
// license in the parent h2transport directory.

package http2

import (
	"net/http"
	"time"
)

type requestHeadersCompletedContextKey struct{}

// RequestHeadersCompletedAt reports when the initial header block was fully
// received. It remains attached while a handler waits for a stream slot.
func RequestHeadersCompletedAt(r *http.Request) (time.Time, bool) {
	completedAt, ok := r.Context().Value(requestHeadersCompletedContextKey{}).(time.Time)
	return completedAt, ok
}
