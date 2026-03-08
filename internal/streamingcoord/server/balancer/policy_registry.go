package balancer

import (
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// policiesBuilders is a map of registered balancer policiesBuilders.
var policiesBuilders typeutil.ConcurrentMap[string, PolicyBuilder]

// newCommonBalancePolicyConfig returns the common balance policy config.
func newCommonBalancePolicyConfig() CommonBalancePolicyConfig {
	params := paramtable.Get()
	return CommonBalancePolicyConfig{
		AllowRebalance:                     params.StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool(),
		AllowRebalanceRecoveryLagThreshold: params.StreamingCfg.WALBalancerPolicyAllowRebalanceRecoveryLagThreshold.GetAsDurationByParse(),
		MinRebalanceIntervalThreshold:      params.StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.GetAsDurationByParse(),
	}
}

// CommonBalancePolicyConfig is the config for balance policy.
type CommonBalancePolicyConfig struct {
	AllowRebalance                     bool          // Whether to allow rebalance.
	AllowRebalanceRecoveryLagThreshold time.Duration // The threshold of recovery lag for balance.
	MinRebalanceIntervalThreshold      time.Duration // The min interval of rebalance.
}

// CurrentLayout is the full topology of streaming node and pChannel.
type CurrentLayout struct {
	Config             CommonBalancePolicyConfig
	Channels           map[channel.ChannelID]types.PChannelInfo
	Stats              map[channel.ChannelID]channel.PChannelStatsView
	AllNodesInfo       map[int64]types.StreamingNodeStatus    // AllNodesInfo is the full information of all available streaming nodes and related pchannels (contain the node not assign anything on it).
	ChannelsToNodes    map[types.ChannelID]int64              // ChannelsToNodes maps assigned channel name to node id.
	ExpectedAccessMode map[channel.ChannelID]types.AccessMode // ExpectedAccessMode is the expected access mode of all channel.
}

// TotalChannels returns the total number of channels in the layout.
func (layout *CurrentLayout) TotalChannels() int {
	return len(layout.Channels)
}

// TotalVChannels returns the total number of vchannels in the layout.
func (layout *CurrentLayout) TotalVChannels() int {
	cnt := 0
	for _, stats := range layout.Stats {
		cnt += len(stats.VChannels)
	}
	return cnt
}

// TotalVChannelPerCollection returns the total number of vchannels per collection in the layout.
func (layout *CurrentLayout) TotalVChannelsOfCollection() map[int64]int {
	cnt := make(map[int64]int)
	for _, stats := range layout.Stats {
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

// AllowRebalance returns true if the balance of the pchannel is allowed.
func (layout *CurrentLayout) AllowRebalance(channelID channel.ChannelID) bool {
	if !layout.Config.AllowRebalance {
		// If rebalance is not allowed, return false directly.
		return false
	}

	// If the last assign timestamp is too close to the current time, rebalance is not allowed.
	if time.Since(layout.Stats[channelID].LastAssignTimestamp) < layout.Config.MinRebalanceIntervalThreshold {
		return false
	}

	// If reach the recovery lag threshold, rebalance is not allowed.
	return !layout.isReachTheRecoveryLagThreshold(channelID)
}

// isReachTheRecoveryLagThreshold returns true if the recovery lag of the pchannel is greater than the recovery lag threshold.
func (layout *CurrentLayout) isReachTheRecoveryLagThreshold(channelID channel.ChannelID) bool {
	balanceAttr := layout.GetWALMetrics(channelID)
	if balanceAttr == nil {
		return false
	}
	r, ok := balanceAttr.(types.RWWALMetrics)
	if !ok {
		return false
	}
	return r.RecoveryLag() > layout.Config.AllowRebalanceRecoveryLagThreshold
}

// GetWALMetrics returns the WAL metrics of the pchannel.
func (layout *CurrentLayout) GetWALMetrics(channelID channel.ChannelID) types.WALMetrics {
	node, ok := layout.ChannelsToNodes[channelID]
	if !ok {
		return nil
	}
	return layout.AllNodesInfo[node].Metrics.WALMetrics[channelID]
}

// GetAllPChannelsSortedByVChannelCountDesc returns all pchannels sorted by vchannel count in descending order.
func (layout *CurrentLayout) GetAllPChannelsSortedByVChannelCountDesc() []types.ChannelID {
	sorter := make(byVChannelCountDesc, 0, layout.TotalChannels())
	for id, stats := range layout.Stats {
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
	ChannelAssignment map[types.ChannelID]types.PChannelInfoAssigned // ChannelAssignment is the assignment of channel to node.
}

// String returns the string representation of the expected layout.
func (layout ExpectedLayout) String() string {
	ss := make([]string, 0, len(layout.ChannelAssignment))
	for _, assignment := range layout.ChannelAssignment {
		ss = append(ss, assignment.String())
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
