package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TrieChecker checks if a Trie index can be built.
type TrieChecker struct {
	scalarIndexChecker
}

func (c *TrieChecker) CheckTrain(params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *TrieChecker) CheckValidDataType(dType schemapb.DataType) error {
	if !typeutil.IsStringType(dType) {
		return fmt.Errorf("trie are only supported on varchar field")
	}
	return nil
}

func newTrieChecker() *TrieChecker {
	return &TrieChecker{}
}
