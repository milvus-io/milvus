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
	timestamp    typeutil.Timestamp
	typeCode     EventTypeCode
	serverID     int32
	eventLength  int32
	nextPosition int32
}

func (header *baseEventHeader) GetMemoryUsageInBytes() int32 {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, header)
	return int32(buf.Len())
}

func (header *baseEventHeader) TypeCode() EventTypeCode {
	return header.typeCode
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
		timestamp: tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
		typeCode:  DescriptorEventType,
		serverID:  ServerID,
	}
	header.eventLength = header.GetMemoryUsageInBytes()
	header.nextPosition = header.eventLength + 4
	return header
}

func newEventHeader(eventTypeCode EventTypeCode) eventHeader {
	return eventHeader{
		baseEventHeader: baseEventHeader{
			timestamp:    tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
			typeCode:     eventTypeCode,
			serverID:     ServerID,
			eventLength:  -1,
			nextPosition: -1,
		},
	}
}
