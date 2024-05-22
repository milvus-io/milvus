package balancer

var _ Policy = &channelCountFairPolicy{}

// Policy is a interface to define the policy of rebalance.
// 1. Must handle the inconsistency of layout.
// 2. Layout should not be modified in policy.
// 3. Optional: Move the channel to another log node to balance the load.
type Policy interface {
	// Balance is a function to balance the load of log node.
	// return a map of channel to a list of balance operation.
	// All balance operation in a list will be executed in order.
	// different channel's balance operation can be executed concurrently.
	Balance(lh *BalanceOPBuilder) map[string][]BalanceOP
}
