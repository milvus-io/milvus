package ack

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// details that sorted by timestamp.
type sortedDetails []*AckDetail

// NewAckDetails creates a new AckDetails.
func NewAckDetails() *AckDetails {
	return &AckDetails{
		detail: make([]*AckDetail, 0),
	}
}

// AckDetails records the information of AckDetail.
// Used to analyze the all acknowledged details.
// TODO: add more analysis methods. e.g. such as counter function with filter.
type AckDetails struct {
	detail []*AckDetail
}

// AddDetails adds details to AckDetails.
// The input details must be sorted by timestamp.
func (ad *AckDetails) AddDetails(details sortedDetails) {
	if len(details) == 0 {
		return
	}
	if len(ad.detail) == 0 {
		ad.detail = details
		return
	}
	if ad.detail[len(ad.detail)-1].BeginTimestamp >= details[0].BeginTimestamp {
		panic("unreachable: the details must be sorted by timestamp")
	}
	ad.detail = append(ad.detail, details...)
}

// Empty returns true if the AckDetails is empty.
func (ad *AckDetails) Empty() bool {
	return len(ad.detail) == 0
}

// Len returns the count of AckDetail.
func (ad *AckDetails) Len() int {
	return len(ad.detail)
}

// IsNoPersistedMessage returns true if no persisted message.
func (ad *AckDetails) IsNoPersistedMessage() bool {
	for _, detail := range ad.detail {
		// only sync message do not persist.
		// it just sync up the timetick with rootcoord
		if !detail.IsSync {
			return false
		}
	}
	return true
}

// LastAllAcknowledgedTimestamp returns the last timestamp which all timestamps before it have been acknowledged.
// panic if no timestamp has been acknowledged.
func (ad *AckDetails) LastAllAcknowledgedTimestamp() uint64 {
	if len(ad.detail) > 0 {
		return ad.detail[len(ad.detail)-1].BeginTimestamp
	}
	return 0
}

// EarliestLastConfirmedMessageID returns the last confirmed message id.
func (ad *AckDetails) EarliestLastConfirmedMessageID() message.MessageID {
	// use the earliest last confirmed message id.
	var msgID message.MessageID
	for _, detail := range ad.detail {
		if msgID == nil {
			msgID = detail.LastConfirmedMessageID
			continue
		}
		if detail.LastConfirmedMessageID != nil && detail.LastConfirmedMessageID.LT(msgID) {
			msgID = detail.LastConfirmedMessageID
		}
	}
	return msgID
}

// Clear clears the AckDetails.
func (ad *AckDetails) Clear() {
	ad.detail = nil
}

// Range iterates the AckDetail.
func (ad *AckDetails) Range(fn func(detail *AckDetail) bool) {
	for _, detail := range ad.detail {
		if !fn(detail) {
			break
		}
	}
}
