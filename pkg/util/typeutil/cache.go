package typeutil

type CacheOpType int32

const (
	CacheAddUserToRole CacheOpType = iota + 1
	CacheRemoveUserFromRole
	CacheGrantPrivilege
	CacheRevokePrivilege
)

type CacheOp struct {
	OpType CacheOpType
	OpKey  string
}
