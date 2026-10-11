package fastpb

import (
	"unicode/utf8"
	"unsafe"
)

// validUTF8 reports whether v is valid UTF-8 (matches proto3 string validation).
func validUTF8(v []byte) bool { return utf8.Valid(v) }

// This file isolates the only unsafe in the package. Both helpers are
// little-endian (x86/arm64) and produce freshly-allocated, self-owned memory —
// they do NOT alias the source buffer, so they are safe regardless of the
// source buffer's lifetime (gRPC-pooled or owned). A separate zero-copy
// aliasing variant (for the owned sliced_blob path) can be added later.

// bytesToF32 returns a new []float32 holding the little-endian float32s in v
// via a single memcpy (len(v) must be a multiple of 4).
func bytesToF32(v []byte) []float32 {
	n := len(v) / 4
	out := make([]float32, n)
	if n > 0 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&out[0])), n*4), v)
	}
	return out
}

// bytesToF64 returns a new []float64 holding the little-endian float64s in v
// via a single memcpy (len(v) must be a multiple of 8).
func bytesToF64(v []byte) []float64 {
	n := len(v) / 8
	out := make([]float64, n)
	if n > 0 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&out[0])), n*8), v)
	}
	return out
}

// strings copies the string elements out of one StringArray submessage into
// a single backing byte arena and one backing []string, collapsing N per-string
// allocations into 2. Strings point into the arena via unsafe.String; the arena
// never reallocates (cap = upper bound), so the pointers stay valid for the life
// of the returned slice. Validates UTF-8 per element when d.utf8 is set.
//
// The bool return is false if the StringArray carries any field other than the
// `data` field (field 1) — the caller then bails to the official codec so the
// unknown/future field is preserved exactly.
func (d dec) strings(b []byte) ([]string, bool, error) {
	// pass 1: count elements (field 1) and total string bytes
	count, total := 0, 0
	p := b
	for len(p) > 0 {
		num, wtype, n := consumeTag(p)
		if n <= 0 {
			return nil, false, errMalformed
		}
		p = p[n:]
		if num != 1 || wtype != 2 {
			return nil, false, nil // unknown field present → caller falls back
		}
		v, n := consumeBytes(p)
		if n <= 0 {
			return nil, false, errMalformed
		}
		count++
		total += len(v)
		p = p[n:]
	}
	if count == 0 {
		return nil, true, nil
	}
	out := make([]string, 0, count)
	arena := make([]byte, 0, total)
	// pass 2: fill
	p = b
	for len(p) > 0 {
		_, _, n := consumeTag(p)
		p = p[n:]
		v, n := consumeBytes(p)
		if d.utf8 && !validUTF8(v) {
			return nil, false, errInvalidUTF8
		}
		if len(v) == 0 {
			out = append(out, "")
		} else {
			off := len(arena)
			arena = append(arena, v...) // never reallocates: cap == total
			out = append(out, unsafe.String(&arena[off], len(v)))
		}
		p = p[n:]
	}
	return out, true, nil
}
