package flowgraph

type Msg interface {
	TimeTick() Timestamp
	DownStreamNodeIdx() int32
}
