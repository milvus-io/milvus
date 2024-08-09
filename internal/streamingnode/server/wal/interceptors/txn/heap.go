package txn

// txnSessionHeapArrayOrderByEndTSO is the heap array of the txnSession.
type txnSessionHeapArrayOrderByEndTSO []*TxnSession

func (h txnSessionHeapArrayOrderByEndTSO) Len() int {
	return len(h)
}

func (h txnSessionHeapArrayOrderByEndTSO) Less(i, j int) bool {
	return h[i].EndTSO() < h[j].EndTSO()
}

func (h txnSessionHeapArrayOrderByEndTSO) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *txnSessionHeapArrayOrderByEndTSO) Push(x interface{}) {
	*h = append(*h, x.(*TxnSession))
}

// Pop pop the last one at len.
func (h *txnSessionHeapArrayOrderByEndTSO) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *txnSessionHeapArrayOrderByEndTSO) Peek() interface{} {
	return (*h)[0]
}
