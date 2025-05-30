//go:build test
// +build test

package snmanager

func ResetStreamingNodeManager() {
	StaticStreamingNodeManager = newStreamingNodeManager()
}
