package coordview

import "github.com/cockroachdb/errors"

// errDataVersionRollback signals that an update would roll a view's
// DataVersion backwards. It is an identity sentinel (not a merr code): the
// check is a local same-package invariant, never compared across component
// boundaries, so a plain sentinel keeps errors.Is exact.
var errDataVersionRollback = errors.New("new data version must not be lower than any existing view's data version")
