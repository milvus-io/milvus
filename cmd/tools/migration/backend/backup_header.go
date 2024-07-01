package backend

import (
	"encoding/json"

	"github.com/milvus-io/milvus/cmd/tools/migration/console"
)

type BackupHeaderVersion int32

const (
	BackupHeaderVersionV1 BackupHeaderVersion = iota
)

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
	v := newDefaultBackupHeaderExtra()
	err := json.Unmarshal(extra, v)
	if err != nil {
		console.Error(err.Error())
		return v
	}
	return v
}
