package backend

import (
	"encoding/json"

	"github.com/milvus-io/milvus/cmd/tools/migration/console"

	"github.com/golang/protobuf/proto"
)

type BackupHeaderVersion int32

const (
	BackupHeaderVersionV1 BackupHeaderVersion = iota
)

// BackupHeader stores etcd backup header information
type BackupHeader struct {
	// Version number for backup format
	Version BackupHeaderVersion `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	// instance name, as rootPath for key prefix
	Instance string `protobuf:"bytes,2,opt,name=instance,proto3" json:"instance,omitempty"`
	// MetaPath used in keys
	MetaPath string `protobuf:"bytes,3,opt,name=meta_path,proto3" json:"meta_path,omitempty"`
	// Entries record number of key-value in backup
	Entries int64 `protobuf:"varint,4,opt,name=entries,proto3" json:"entries,omitempty"`
	// Component is the backup target
	Component string `protobuf:"bytes,5,opt,name=component,proto3" json:"component,omitempty"`
	// Extra property reserved
	Extra []byte `protobuf:"bytes,6,opt,name=extra,proto3" json:"-"`
}

func (v *BackupHeader) Reset() {
	*v = BackupHeader{}
}

func (v *BackupHeader) String() string {
	return proto.CompactTextString(v)
}

func (v *BackupHeader) ProtoMessage() {}

type BackupHeaderExtra struct {
	EntryIncludeRootPath bool `json:"entry_include_root_path"`
}

type extraOption func(extra *BackupHeaderExtra)

func setEntryIncludeRootPath(include bool) extraOption {
	return func(extra *BackupHeaderExtra) {
		extra.EntryIncludeRootPath = include
	}
}

func newDefaultBackupHeaderExtra() *BackupHeaderExtra {
	return &BackupHeaderExtra{EntryIncludeRootPath: false}
}

func newBackupHeaderExtra(opts ...extraOption) *BackupHeaderExtra {
	v := newDefaultBackupHeaderExtra()
	v.apply(opts...)
	return v
}

func (v *BackupHeaderExtra) apply(opts ...extraOption) {
	for _, opt := range opts {
		opt(v)
	}
}

func (v *BackupHeaderExtra) ToJSONBytes() []byte {
	bs, err := json.Marshal(v)
	if err != nil {
		console.Error(err.Error())
		return nil
	}
	return bs
}

func GetExtra(extra []byte) *BackupHeaderExtra {
	var v = newDefaultBackupHeaderExtra()
	err := json.Unmarshal(extra, v)
	if err != nil {
		console.Error(err.Error())
		return v
	}
	return v
}
