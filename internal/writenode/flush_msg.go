package writenode

type (
	// segID: set when flushComplete == true, to tell
	//  the flush_sync_service which segFlush msg does this
	//  DDL flush for, so that ddl flush and insert flush
	//  will sync.
	ddlBinlogPathMsg struct {
		collID UniqueID
		segID  UniqueID
		paths  []string
	}

	ddlFlushSyncMsg struct {
		ddlBinlogPathMsg
		flushCompleted bool
	}

	insertBinlogPathMsg struct {
		ts      Timestamp
		segID   UniqueID
		fieldID int32 // TODO GOOSE may need to change
		paths   []string
	}

	// This Msg can notify flushSyncService
	//   1.To append binary logs
	//   2.To set flush-completed status
	//
	// When `flushComplete == false`
	//     `ts` means OpenTime of a segFlushMeta
	// When `flushComplete == true`
	//     `ts` means CloseTime of a segFlushMeta,
	//     `fieldID` and `paths` need to be empty
	insertFlushSyncMsg struct {
		insertBinlogPathMsg
		flushCompleted bool
	}
)
