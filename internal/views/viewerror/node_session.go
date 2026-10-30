package viewerror

// NodeSessionUnavailable is client-local metadata, never a wire error. Changed
// belongs to the QN session watch captured before dispatch, so a resolver update
// racing with an RPC failure cannot be missed. Unwrap preserves the RPC cause.
type NodeSessionUnavailable struct {
	Cause   error
	Changed <-chan struct{}
}

func (e *NodeSessionUnavailable) Error() string { return e.Cause.Error() }
func (e *NodeSessionUnavailable) Unwrap() error { return e.Cause }
