package timestamp

// newDefaultAckDetail creates a new default timestampAckDetail.
func newDefaultAckDetail(ts uint64) *AckDetail {
	return &AckDetail{
		Timestamp: ts,
		IsSync:    false,
		Err:       nil,
	}
}

// AckDetail records the information of timestampAck.
// TODO: fill it up.
type AckDetail struct {
	Timestamp uint64
	IsSync    bool
	Err       error
}

type AckOption func(*AckDetail)

// OptSync marks the timestampAck is sync message.
func OptSync() AckOption {
	return func(detail *AckDetail) {
		detail.IsSync = true
	}
}

// OptError marks the timestamp ack with error info.
func OptError(err error) AckOption {
	return func(detail *AckDetail) {
		detail.Err = err
	}
}
