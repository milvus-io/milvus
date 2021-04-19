package masterservice

// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...

const (
	StartOfUserFieldID = 100
	RowIDField         = 0
	TimeStampField     = 1
	RowIDFieldName     = "RowID"
	TimeStampFieldName = "Timestamp"
)
