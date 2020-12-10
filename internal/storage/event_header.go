package storage

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type baseEventHeader struct {
	Timestamp    typeutil.Timestamp
	TypeCode     EventTypeCode
	ServerID     int32
	EventLength  int32
	NextPosition int32
}

func (header *baseEventHeader) GetMemoryUsageInBytes() int32 {
	return int32(binary.Size(header))
}

func (header *baseEventHeader) Write(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, header)
}

type descriptorEventHeader = baseEventHeader

type eventHeader struct {
	baseEventHeader
}

func readEventHeader(buffer io.Reader) (*eventHeader, error) {
	header := &eventHeader{}
	if err := binary.Read(buffer, binary.LittleEndian, header); err != nil {
		return nil, err
	}

	return header, nil
}

func readDescriptorEventHeader(buffer io.Reader) (*descriptorEventHeader, error) {
	header := &descriptorEventHeader{}
	if err := binary.Read(buffer, binary.LittleEndian, header); err != nil {
		return nil, err
	}
	return header, nil
}

func newDescriptorEventHeader() (*descriptorEventHeader, error) {
	header := descriptorEventHeader{
		Timestamp: tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
		TypeCode:  DescriptorEventType,
		ServerID:  ServerID,
	}
	return &header, nil
}

func newEventHeader(eventTypeCode EventTypeCode) (*eventHeader, error) {
	return &eventHeader{
		baseEventHeader: baseEventHeader{
			Timestamp:    tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
			TypeCode:     eventTypeCode,
			ServerID:     ServerID,
			EventLength:  -1,
			NextPosition: -1,
		},
	}, nil
}
