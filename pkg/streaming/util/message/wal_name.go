package message

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type WALName commonpb.WALName

const (
	WALNameUnknown    WALName = WALName(commonpb.WALName_Unknown)
	WALNameRocksmq    WALName = WALName(commonpb.WALName_RocksMQ)
	WALNameKafka      WALName = WALName(commonpb.WALName_Kafka)
	WALNamePulsar     WALName = WALName(commonpb.WALName_Pulsar)
	WALNameWoodpecker WALName = WALName(commonpb.WALName_WoodPecker)
	WALNameTest       WALName = WALName(commonpb.WALName_Test)
)

var defaultWALName = atomic.NewPointer[WALName](nil)

// RegisterDefaultWALName register the default WALName.
func RegisterDefaultWALName(walName WALName) {
	defaultWALName.Store(&walName)
}

// MustGetDefaultWALName get the default WALName.
func MustGetDefaultWALName() WALName {
	return *defaultWALName.Load()
}

// NewWALName create a WALName from string.
func NewWALName(name string) WALName {
	for wal, n := range walNameMap {
		if n == name {
			return wal
		}
	}
	return WALNameUnknown
}

// walNameMap is the mapping from WALName to string.
var walNameMap = map[WALName]string{
	WALNameUnknown:    "unknown",
	WALNameRocksmq:    "rocksmq",
	WALNameKafka:      "kafka",
	WALNamePulsar:     "pulsar",
	WALNameWoodpecker: "woodpecker",
	WALNameTest:       "walimplstest",
}

// String returns the string representation of the WALName.
func (w WALName) String() string {
	return walNameMap[w]
}
