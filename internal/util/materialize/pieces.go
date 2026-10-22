package materialize

import (
	"sort"
	"strings"
)

// pieces is a byte sequence held as consecutive parts: a body entry as the
// release codec received it, split wherever gRPC's receive buffers split it
// (resource.MaterializeBody), or one contiguous part.
type pieces struct {
	parts [][]byte
	// starts[i] is parts[i]'s offset in the sequence; starts[len(parts)] is
	// its length.
	starts []int
}

func newPieces(parts [][]byte) *pieces {
	p := &pieces{parts: parts, starts: make([]int, len(parts)+1)}
	for i, b := range parts {
		p.starts[i+1] = p.starts[i] + len(b)
	}
	return p
}

func (p *pieces) size() int { return p.starts[len(p.parts)] }

// span is a range of a pieces sequence: an Arrow buffer of an entry.
type span struct {
	p      *pieces
	off, n int
}

// view returns the span as one slice when it lies in a single part.
func (s span) view() ([]byte, bool) {
	if s.n == 0 {
		return nil, true
	}
	i := s.p.find(s.off, 0)
	in := s.off - s.p.starts[i]
	if in+s.n > len(s.p.parts[i]) {
		return nil, false
	}
	return s.p.parts[i][in : in+s.n : in+s.n], true
}

// find returns the part holding offset off (< size), trying part hint first.
func (p *pieces) find(off, hint int) int {
	if hint < len(p.parts) && p.starts[hint] <= off && off < p.starts[hint+1] {
		return hint
	}
	if next := hint + 1; next < len(p.parts) && p.starts[next] <= off && off < p.starts[next+1] {
		return next
	}
	// The first part ending after off; empty parts end where they start.
	return sort.Search(len(p.parts), func(i int) bool { return p.starts[i+1] > off })
}

// cursor reads a pieces sequence and remembers the part it read last, so
// reads in increasing order, or of nearby rows, skip the search. Callers
// bound-check every range against the sequence first.
type cursor struct {
	p    *pieces
	part int
}

// read copies len(dst) bytes starting at off into dst; a range across parts
// is copied in pieces.
func (c *cursor) read(dst []byte, off int) {
	for len(dst) > 0 {
		c.part = c.p.find(off, c.part)
		in := off - c.p.starts[c.part]
		n := copy(dst, c.p.parts[c.part][in:])
		dst = dst[n:]
		off += n
	}
}

// writeString appends n bytes starting at off to sb.
func (c *cursor) writeString(sb *strings.Builder, off, n int) {
	for n > 0 {
		c.part = c.p.find(off, c.part)
		in := off - c.p.starts[c.part]
		b := c.p.parts[c.part][in:]
		if len(b) > n {
			b = b[:n]
		}
		sb.Write(b)
		off += len(b)
		n -= len(b)
	}
}

// bytes returns n bytes starting at off: a view when they lie in one part, a
// copy otherwise.
func (c *cursor) bytes(off, n int) []byte {
	if n == 0 {
		return nil
	}
	c.part = c.p.find(off, c.part)
	in := off - c.p.starts[c.part]
	if b := c.p.parts[c.part][in:]; len(b) >= n {
		return b[:n:n]
	}
	out := make([]byte, n)
	c.read(out, off)
	return out
}
