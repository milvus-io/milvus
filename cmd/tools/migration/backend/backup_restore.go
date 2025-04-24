package backend

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type BackupFile []byte

func (f *BackupFile) Reset() {
	*f = nil
}

func (f *BackupFile) writeBytes(bs []byte) {
	*f = append(*f, bs...)
}

func (f *BackupFile) writeHeaderLength(l uint64) {
	lengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lengthBytes, l)
	f.writeBytes(lengthBytes)
}

func (f *BackupFile) writeHeaderBytes(header []byte) {
	f.writeBytes(header)
}

func (f *BackupFile) WriteHeader(header *BackupHeader) error {
	f.Reset()
	marshaledHeader, err := proto.Marshal(header)
	if err != nil {
		return err
	}
	l := len(marshaledHeader)
	f.writeHeaderLength(uint64(l))
	f.writeHeaderBytes(marshaledHeader)
	return nil
}

func (f *BackupFile) writeEntryLength(l uint64) {
	f.writeHeaderLength(l)
}

func (f *BackupFile) writeEntryBytes(entry []byte) {
	f.writeBytes(entry)
}

func (f *BackupFile) WriteEntry(k, v string) error {
	entry := &commonpb.KeyDataPair{
		Key:  k,
		Data: []byte(v),
	}
	marshaledEntry, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	l := len(marshaledEntry)
	f.writeEntryLength(uint64(l))
	f.writeEntryBytes(marshaledEntry)
	return nil
}

func (f *BackupFile) ReadHeader() (header *BackupHeader, headerLength uint64, err error) {
	if len(*f) < 8 {
		return nil, 0, errors.New("invalid backup file, cannot read header length")
	}
	headerLength = binary.LittleEndian.Uint64((*f)[:8])
	if uint64(len(*f)) < 8+headerLength {
		return nil, 0, errors.New("invalid backup file, cannot read header")
	}
	header = &BackupHeader{}
	if err := proto.Unmarshal((*f)[8:headerLength+8], header); err != nil {
		return nil, 0, fmt.Errorf("invalid backup file, cannot read header: %s", err.Error())
	}
	return header, headerLength, nil
}

func (f *BackupFile) ReadEntryFromPos(pos uint64) (entryLength uint64, entry *commonpb.KeyDataPair, err error) {
	if pos == uint64(len(*f)) {
		return 0, nil, io.EOF
	}
	if uint64(len(*f)) < pos+8 {
		return 0, nil, errors.New("invalid backup file, cannot read entry length")
	}
	entryLength = binary.LittleEndian.Uint64((*f)[pos : pos+8])
	if uint64(len(*f)) < pos+8+entryLength {
		return 0, nil, errors.New("invalid backup file, cannot read entry")
	}
	entry = &commonpb.KeyDataPair{}
	if err := proto.Unmarshal((*f)[pos+8:pos+8+entryLength], entry); err != nil {
		return 0, nil, fmt.Errorf("invalid backup file, cannot read entry: %s", err.Error())
	}
	return entryLength, entry, nil
}

func (f *BackupFile) DeSerialize() (header *BackupHeader, kvs map[string]string, err error) {
	pos := uint64(0)
	header, headerLength, err := f.ReadHeader()
	if err != nil {
		return nil, nil, err
	}
	pos += 8 + headerLength
	kvs = make(map[string]string)
	for {
		entryLength, entry, err := f.ReadEntryFromPos(pos)
		if err == io.EOF {
			return header, kvs, nil
		}
		if err != nil {
			return nil, nil, err
		}
		kvs[entry.GetKey()] = string(entry.GetData())
		pos += 8 + entryLength
	}
}

type BackupCodec struct{}

func (c *BackupCodec) Serialize(header *BackupHeader, kvs map[string]string) (BackupFile, error) {
	file := make(BackupFile, 0)
	header.Entries = int64(len(kvs))
	if err := file.WriteHeader(header); err != nil {
		return nil, err
	}
	for k, v := range kvs {
		if err := file.WriteEntry(k, v); err != nil {
			return nil, err
		}
	}
	return file, nil
}

func (c *BackupCodec) DeSerialize(file BackupFile) (header *BackupHeader, kvs map[string]string, err error) {
	return file.DeSerialize()
}

func NewBackupCodec() *BackupCodec {
	return &BackupCodec{}
}
