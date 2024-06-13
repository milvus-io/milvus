package walimplstest

import (
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
)

func TestWALImplsTest(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 100, &openerBuilder{}).Run()
}
