package balance

import (
	"fmt"

	"github.com/samber/lo"
)

// balanceReport is the struct to store balance plan generation detail.
type balanceReport struct {
	// node score information
	// no mut protection, no concurrent safe guaranteed
	// it safe for now since BalanceReport is used only one `Check` lifetime.
	nodeItems map[int64]*nodeItemInfo

	// plain stringer records, String() is deferred utilizing zap.Stringer/Stringers feature
	records       []fmt.Stringer
	detailRecords []fmt.Stringer
}

// NewBalanceReport returns an initialized BalanceReport instance
func NewBalanceReport() *balanceReport {
	return &balanceReport{
		nodeItems: make(map[int64]*nodeItemInfo),
	}
}

func (br *balanceReport) AddRecord(record fmt.Stringer) {
	br.records = append(br.records, record)
	br.detailRecords = append(br.detailRecords, record)
}

func (br *balanceReport) AddDetailRecord(record fmt.Stringer) {
	br.detailRecords = append(br.detailRecords, record)
}

func (br *balanceReport) AddSegmentPlan() {
}

func (br *balanceReport) AddNodeItem(item *nodeItem) {
	_, ok := br.nodeItems[item.nodeID]
	if !ok {
		nodeItem := &nodeItemInfo{
			nodeItem:     item,
			memoryFactor: 1,
		}
		br.nodeItems[item.nodeID] = nodeItem
	}
}

func (br *balanceReport) SetMemoryFactor(node int64, memoryFactor float64) {
	nodeItem, ok := br.nodeItems[node]
	if ok {
		nodeItem.memoryFactor = memoryFactor
	}
}

func (br *balanceReport) SetDelegatorScore(node int64, delegatorScore float64) {
	nodeItem, ok := br.nodeItems[node]
	if ok {
		nodeItem.delegatorScore = delegatorScore
	}
}

func (br *balanceReport) NodesInfo() []fmt.Stringer {
	return lo.Map(lo.Values(br.nodeItems), func(item *nodeItemInfo, _ int) fmt.Stringer {
		return item
	})
}

type nodeItemInfo struct {
	nodeItem       *nodeItem
	delegatorScore float64
	memoryFactor   float64
}

func (info *nodeItemInfo) String() string {
	return fmt.Sprintf("NodeItemInfo %s, memory factor %f, delegator score: %f", info.nodeItem, info.memoryFactor, info.delegatorScore)
}

// strRecord implment fmt.Stringer with simple string.
type strRecord string

func (str strRecord) String() string {
	return string(str)
}

func StrRecord(str string) strRecord { return strRecord(str) }

type strRecordf struct {
	format string
	values []any
}

func (f strRecordf) String() string {
	return fmt.Sprintf(f.format, f.values...)
}

func StrRecordf(format string, values ...any) strRecordf {
	return strRecordf{
		format: format,
		values: values,
	}
}
