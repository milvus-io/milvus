package kv

type TxnKVMock struct {
	TxnKV
	SaveF   func(key, value string) error
	RemoveF func(key string) error
}

func (m TxnKVMock) Load(key string) (string, error) {
	panic("implement me")
}

func (m TxnKVMock) MultiLoad(keys []string) ([]string, error) {
	panic("implement me")
}

func (m TxnKVMock) LoadWithPrefix(key string) ([]string, []string, error) {
	panic("implement me")
}

func (m TxnKVMock) Save(key, value string) error {
	return m.SaveF(key, value)
}

func (m TxnKVMock) MultiSave(kvs map[string]string) error {
	panic("implement me")
}

func (m TxnKVMock) Remove(key string) error {
	return m.RemoveF(key)
}

func (m TxnKVMock) MultiRemove(keys []string) error {
	panic("implement me")
}

func (m TxnKVMock) RemoveWithPrefix(key string) error {
	panic("implement me")
}

func (m TxnKVMock) Close() {
	panic("implement me")
}

func (m TxnKVMock) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	panic("implement me")
}

func (m TxnKVMock) MultiRemoveWithPrefix(keys []string) error {
	panic("implement me")
}

func (m TxnKVMock) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	panic("implement me")
}

func NewMockTxnKV() *TxnKVMock {
	return &TxnKVMock{}
}
