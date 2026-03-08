package typeutil

// IsChanClosed returns whether input signal channel is closed or not.
// this method accept `chan struct{}` type only in case of passing msg channels by mistake.
func IsChanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
