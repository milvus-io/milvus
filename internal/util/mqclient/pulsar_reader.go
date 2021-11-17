package mqclient

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarReader struct {
	r pulsar.Reader
}

func (pr *pulsarReader) Topic() string {
	return pr.r.Topic()
}

func (pr *pulsarReader) Next(ctx context.Context) (Message, error) {
	pm, err := pr.r.Next(ctx)
	if err != nil {
		return nil, err
	}

	return &pulsarMessage{msg: pm}, nil
}

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
