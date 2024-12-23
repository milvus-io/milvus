package client

import "github.com/cockroachdb/errors"

var (
	// magicPrefix is used to identify the rocksmq legacy message and new message for streaming service.
	// Make a low probability of collision with the legacy proto message.
	magicPrefix = append([]byte{0xFF, 0xFE, 0xFD, 0xFC}, []byte("STREAM")...)
	errNotStreamingServiceMessage = errors.New("not a streaming service message")
)
