package storage

import (
	"bytes"
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
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, header)
	return int32(buf.Len())
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

func newDescriptorEventHeader() descriptorEventHeader {
	header := descriptorEventHeader{
		Timestamp: tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
		TypeCode:  DescriptorEventType,
		ServerID:  ServerID,
	}
	header.EventLength = header.GetMemoryUsageInBytes()
	header.NextPosition = header.EventLength + 4
	return header
}

func newEventHeader(eventTypeCode EventTypeCode) eventHeader {
	return eventHeader{
		baseEventHeader: baseEventHeader{
			Timestamp:    tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
			TypeCode:     eventTypeCode,
			ServerID:     ServerID,
			EventLength:  -1,
			NextPosition: -1,
		},
	}
}
