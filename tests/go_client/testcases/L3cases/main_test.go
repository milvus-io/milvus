package L3cases

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestMain(m *testing.M) {
	os.Exit(helper.RunTests(m))
}
