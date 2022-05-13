package typeutil

type CacheOpType int32

const (
	CacheAdd    CacheOpType = 1
	CacheRemove CacheOpType = 2
)

type CacheOp struct {
	OpType CacheOpType
	OpKey  string
}
