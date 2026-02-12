//go:build go1.26 && !go1.27
// +build go1.26,!go1.27

package tool

// gGoroutineIDOffset is the same as Go 1.25 (152 bytes into g struct)
const gGoroutineIDOffset = 152
