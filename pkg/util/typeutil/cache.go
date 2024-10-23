package typeutil

type CacheOpType int32

const (
	CacheAddUserToRole CacheOpType = iota + 1
	CacheRemoveUserFromRole
	CacheGrantPrivilege
	CacheRevokePrivilege
	CacheDeleteUser
	CacheDropRole
	CacheRefresh
	CacheCreatePrivilegeGroup
	CacheDropPrivilegeGroup
	CacheListPrivilegeGroups
	CacheAddPrivilegesToGroup
	CacheDropPrivilegesFromGroup
)

type CacheOp struct {
	OpType CacheOpType
	OpKey  string
}
