package viewerror

// NodeSessionUnavailable is client-local metadata, never a wire error. Changed
// belongs to the QN picker revision captured before dispatch, so a picker update
// racing with an RPC failure cannot be missed. Unwrap preserves the RPC cause.
type NodeSessionUnavailable struct {
	Cause   error
	Changed <-chan struct{}
	// Gone requires observing this exact session disappear from discovery.
	// A missing picker entry alone is not sufficient evidence.
	Gone bool
}

func (e *NodeSessionUnavailable) Error() string { return e.Cause.Error() }
func (e *NodeSessionUnavailable) Unwrap() error { return e.Cause }
