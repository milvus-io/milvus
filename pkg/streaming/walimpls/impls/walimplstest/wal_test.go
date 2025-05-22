package walimplstest

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
)

func TestWALImplsTest(t *testing.T) {
	enableFenceError.Store(false)
	defer enableFenceError.Store(true)
	walimpls.NewWALImplsTestFramework(t, 100, &openerBuilder{}).Run()
}
