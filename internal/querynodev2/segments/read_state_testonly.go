//go:build test

package segments

import "github.com/milvus-io/milvus/internal/util/segcore"

// SealedReadVersionCountForTest inspects the native record of a pinned View fork.
func SealedReadVersionCountForTest(read Segment) (uint64, error) {
	return segcore.UpdateVersionCountForTest(read.(*sealedReadState).native)
}
