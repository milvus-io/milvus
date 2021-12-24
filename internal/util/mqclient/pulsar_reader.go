package mqclient

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

// pulsarReader contains a pulsar reader
type pulsarReader struct {
	r pulsar.Reader
}

// Topic returns the topic of pulsar reader
func (pr *pulsarReader) Topic() string {
	return pr.r.Topic()
}

// Next read the next message in the topic, blocking until a message is available
func (pr *pulsarReader) Next(ctx context.Context) (Message, error) {
	pm, err := pr.r.Next(ctx)
	if err != nil {
		return nil, err
	}

	return &pulsarMessage{msg: pm}, nil
}

// HasNext check if there is any message available to read from the current position
func (pr *pulsarReader) HasNext() bool {
	return pr.r.HasNext()
}

func (pr *pulsarReader) Close() {
	pr.r.Close()
}

func (pr *pulsarReader) Seek(id MessageID) error {
	messageID := id.(*pulsarID).messageID
	err := pr.r.Seek(messageID)
	if err != nil {
		return err
	}
	return nil
}
