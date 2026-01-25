//go:build test
// +build test

package registry

func ResetRegistration() {
	resetMessageAckCallbacks()
	resetMessageAckOnceCallbacks()
}
