package timetick

import "github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"

// ackDetails records the information of AckDetail.
// Used to analyze the ack details.
// TODO: add more analysis methods. e.g. such as counter function with filter.
type ackDetails struct {
	detail []*timestamp.AckDetail
}

// AddDetails adds details to AckDetails.
func (ad *ackDetails) AddDetails(details []*timestamp.AckDetail) {
	if len(details) == 0 {
		return
	}
	if len(ad.detail) == 0 {
		ad.detail = details
		return
	}
	ad.detail = append(ad.detail, details...)
}

// Empty returns true if the AckDetails is empty.
func (ad *ackDetails) Empty() bool {
	return len(ad.detail) == 0
}

// Len returns the count of AckDetail.
func (ad *ackDetails) Len() int {
	return len(ad.detail)
}

// LastAllAcknowledgedTimestamp returns the last timestamp which all timestamps before it have been acknowledged.
// panic if no timestamp has been acknowledged.
func (ad *ackDetails) LastAllAcknowledgedTimestamp() uint64 {
	return ad.detail[len(ad.detail)-1].Timestamp
}

// Clear clears the AckDetails.
func (ad *ackDetails) Clear() {
	ad.detail = nil
}
