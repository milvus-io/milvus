package master

import (
	"log"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type getSysConfigsTask struct {
	baseTask
	configkv *kv.EtcdKV
	req      *internalpb.SysConfigRequest
	keys     []string
	values   []string
}

func (t *getSysConfigsTask) Type() internalpb.MsgType {
	if t.req == nil {
		log.Printf("null request")
		return 0
	}
	return t.req.MsgType
}

func (t *getSysConfigsTask) Ts() (Timestamp, error) {
	if t.req == nil {
		return 0, errors.New("null request")
	}
	return t.req.Timestamp, nil
}

func (t *getSysConfigsTask) Execute() error {
	if t.req == nil {
		return errors.New("null request")
	}

	sc := &SysConfig{kv: t.configkv}
	keyMap := make(map[string]bool)

	// Load configs with prefix
	for _, prefix := range t.req.KeyPrefixes {
		prefixKeys, prefixVals, err := sc.GetByPrefix(prefix)
		if err != nil {
			return errors.Errorf("Load configs by prefix wrong: %s", err.Error())
		}
		t.keys = append(t.keys, prefixKeys...)
		t.values = append(t.values, prefixVals...)
	}

	for _, key := range t.keys {
		keyMap[key] = true
	}

	// Load specific configs
	if len(t.req.Keys) > 0 {
		// To clean up duplicated keys
		cleanKeys := []string{}
		for _, key := range t.req.Keys {
			if v, ok := keyMap[key]; (!ok) || (ok && !v) {
				cleanKeys = append(cleanKeys, key)
				keyMap[key] = true
				continue
			}
			log.Println("[GetSysConfigs] Warning: duplicate key:", key)
		}

		v, err := sc.Get(cleanKeys)
		if err != nil {
			return errors.Errorf("Load configs wrong: %s", err.Error())
		}

		t.keys = append(t.keys, cleanKeys...)
		t.values = append(t.values, v...)
	}

	return nil
}
