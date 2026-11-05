//go:build test && dynamic

package snview

// ReplicaDirectoryForTest exposes the real directory to cross-package transport tests.
type ReplicaDirectoryForTest = replicaDirectory

func NewReplicaDirectoryForTest() *ReplicaDirectoryForTest {
	return newReplicaDirectory()
}
