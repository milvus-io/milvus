//go:build !cgo

package textmatch

// ComputePhraseMatchSlop is a dummy fallback implementation for when CGO is disabled.
func ComputePhraseMatchSlop(analyzerParams string, query string, data string) (int32, error) {
	return 0, nil
}
