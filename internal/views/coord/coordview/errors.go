package coordview

import "github.com/milvus-io/milvus/pkg/v2/util/merr"

var errDataVersionRollback = merr.WrapErrServiceInternal("new data version must not be lower than any existing view's data version")
