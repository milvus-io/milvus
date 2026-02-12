//go:build go1.26 && !go1.27
// +build go1.26,!go1.27

package sysmon

// sysmonLockOffset is the same as Go 1.25 (336 bytes into schedt)
const (
	sysmonLockOffset = 336
)
