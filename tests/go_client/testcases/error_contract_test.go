package testcases

import (
	"testing"
	"time"

	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestErrorContract demonstrates asserting the structured error contract
// (code, is_input_error, retriable) end-to-end against a real server, while
// still pinning the message substring for per-error specificity. These double
// as living examples of the CheckErrTriple / CheckErrCode helpers.
func TestErrorContract(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	t.Run("empty_collection_name_is_input_error", func(t *testing.T) {
		schema := genDefaultSchema()
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption("", schema))
		// Malformed request: the caller's fault, never retriable. The message
		// substring pins which parameter error this is (code 1100 is shared by
		// every parameter error).
		common.CheckErrTriple(t, err, merr.ErrParameterInvalid, true, false, "collection name should not be empty")
	})

	t.Run("describe_missing_collection_is_input_error", func(t *testing.T) {
		name := common.GenRandomString("missing", 6)
		_, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
		// ErrCollectionNotFound is SystemError by default, but the proxy stamps
		// it InputError for user-supplied names via WrapErrAsInputErrorWhen.
		common.CheckErrTriple(t, err, merr.ErrCollectionNotFound, true, false, "can't find collection")
	})
}
