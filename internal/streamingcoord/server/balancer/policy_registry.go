package balancer

import (
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// policiesBuilders is a map of registered balancer policiesBuilders.
var policiesBuilders typeutil.ConcurrentMap[string, PolicyBuilder]

// CurrentLayout is the full topology of streaming node and pChannel.
type CurrentLayout struct {
	Channels        map[types.ChannelID]channel.PChannelStatsView // Stats is the statistics of all pchannels.
	AllNodesInfo    map[int64]types.StreamingNodeInfo             // AllNodesInfo is the full information of all available streaming nodes and related pchannels (contain the node not assign anything on it).
	ChannelsToNodes map[types.ChannelID]int64                     // ChannelsToNodes maps assigned channel name to node id.
}

// TotalChannels returns the total number of channels in the layout.
func (layout *CurrentLayout) TotalChannels() int {
	return len(layout.Channels)
}

// TotalVChannels returns the total number of vchannels in the layout.
func (layout *CurrentLayout) TotalVChannels() int {
	cnt := 0
	for _, stats := range layout.Channels {
		cnt += len(stats.VChannels)
	}
	return cnt
}

// TotalVChannelPerCollection returns the total number of vchannels per collection in the layout.
func (layout *CurrentLayout) TotalVChannelsOfCollection() map[int64]int {
	cnt := make(map[int64]int)
	for _, stats := range layout.Channels {
		for _, collectionID := range stats.VChannels {
			cnt[collectionID]++
		}
	}
	return cnt
}

// TotalNodes returns the total number of nodes in the layout.
func (layout *CurrentLayout) TotalNodes() int {
	return len(layout.AllNodesInfo)
}

// GetAllPChannelsSortedByVChannelCountDesc returns all pchannels sorted by vchannel count in descending order.
func (layout *CurrentLayout) GetAllPChannelsSortedByVChannelCountDesc() []types.ChannelID {
	sorter := make(byVChannelCountDesc, 0, layout.TotalChannels())
	for id, stats := range layout.Channels {
		sorter = append(sorter, withVChannelCount{
			id:            id,
			vchannelCount: len(stats.VChannels),
		})
	}
	sort.Sort(sorter)
	return lo.Map([]withVChannelCount(sorter), func(item withVChannelCount, _ int) types.ChannelID {
		return item.id
	})
}

type withVChannelCount struct {
	id            types.ChannelID
	vchannelCount int
}

type byVChannelCountDesc []withVChannelCount

func (a byVChannelCountDesc) Len() int { return len(a) }

func (a byVChannelCountDesc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a byVChannelCountDesc) Less(i, j int) bool {
	return a[i].vchannelCount > a[j].vchannelCount || (a[i].vchannelCount == a[j].vchannelCount && a[i].id.LT(a[j].id))
}

// ExpectedLayout is the expected layout of streaming node and pChannel.
type ExpectedLayout struct {
	ChannelAssignment map[types.ChannelID]types.StreamingNodeInfo // ChannelAssignment is the assignment of channel to node.
}

// String returns the string representation of the expected layout.
func (layout ExpectedLayout) String() string {
	ss := make([]string, 0, len(layout.ChannelAssignment))
	for channelID, node := range layout.ChannelAssignment {
		ss = append(ss, channelID.String()+":"+node.String())
	}
	return strings.Join(ss, ",")
}

// PolicyBuilder is a interface to build the policy of rebalance.
type PolicyBuilder interface {
	// Name is the name of the policy.
	Name() string

	// Build is a function to build the policy.
	Build() Policy
}

// Policy is a interface to define the policy of rebalance.
type Policy interface {
	log.LoggerBinder

	// Name is the name of the policy.
	Name() string

	// Balance is a function to balance the load of streaming node.
	// 1. all channel should be assigned.
	// 2. incoming layout should not be changed.
	// 3. return a expected layout.
	// 4. otherwise, error must be returned.
	// return a map of channel to a list of balance operation.
	// All balance operation in a list will be executed in order.
	// different channel's balance operation can be executed concurrently.
	Balance(currentLayout CurrentLayout) (expectedLayout ExpectedLayout, err error)
}

// RegisterPolicy registers balancer policy.
func RegisterPolicy(b PolicyBuilder) {
	_, loaded := policiesBuilders.GetOrInsert(b.Name(), b)
	if loaded {
		panic("policy already registered: " + b.Name())
	}
}

// mustGetPolicy returns the walimpls builder by name.
func mustGetPolicy(name string) PolicyBuilder {
	b, ok := policiesBuilders.Get(name)
	if !ok {
		panic("policy not found: " + name)
	}
	return b
}
