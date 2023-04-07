package common

// ServiceEntryBase base implementation for ServiceEntry.
type ServiceEntryBase struct {
	id       int64
	addr     string
	compType string
}

func (e *ServiceEntryBase) SetID(id int64) {
	e.id = id
}

func (e *ServiceEntryBase) SetAddr(addr string) {
	e.addr = addr
}

func (e *ServiceEntryBase) SetComponentType(compType string) {
	e.compType = compType
}

func (e *ServiceEntryBase) ID() int64 {
	return e.id
}

func (e *ServiceEntryBase) Addr() string {
	return e.addr
}

func (e *ServiceEntryBase) ComponentType() string {
	return e.compType
}

func NewServiceEntryBase(addr string, compType string) ServiceEntryBase {
	return ServiceEntryBase{
		addr:     addr,
		compType: compType,
	}
}
