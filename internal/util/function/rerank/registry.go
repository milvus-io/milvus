package rerank

import "sync"

var (
	registryMu sync.Mutex
	// map collection name -> list of rerankers
	rerankerRegistry = make(map[string][]Reranker)
)

func registerRerankerForCollection(coll string, r Reranker) {
	if coll == "" || r == nil {
		return
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	rerankerRegistry[coll] = append(rerankerRegistry[coll], r)
}

// UnregisterRerankersForCollection closes and removes rerankers associated with a collection name
func UnregisterRerankersForCollection(coll string) {
	if coll == "" {
		return
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if list, ok := rerankerRegistry[coll]; ok {
		for _, r := range list {
			if r != nil {
				r.Close()
			}
		}
		delete(rerankerRegistry, coll)
	}
}
