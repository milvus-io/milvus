package walimplstest

import (
	"testing"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
)

func TestWALImplsTest(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 100, &openerBuilder{}).Run()
}
