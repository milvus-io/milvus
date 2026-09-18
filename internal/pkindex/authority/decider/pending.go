package decider

import "github.com/milvus-io/milvus/internal/pkindex/authority"

// pendingTxn collects the primary key writes of a transaction that has not been
// committed yet. They reach the index only when the CommitTxn is applied.
type pendingTxn struct {
	puts           map[string]pendingPut // encoded key -> the last insert of that key
	deletes        map[string]authority.PK
	cleanupClaimed bool
}

type pendingPut struct {
	pk        authority.PK
	segmentID int64
}

func newPendingTxn() *pendingTxn {
	return &pendingTxn{
		puts:    make(map[string]pendingPut),
		deletes: make(map[string]authority.PK),
	}
}

func (p *pendingTxn) recordInsert(pks []authority.PK, segmentID int64) {
	for _, pk := range pks {
		p.puts[string(pk.Encode())] = pendingPut{pk: pk, segmentID: segmentID}
	}
}

func (p *pendingTxn) recordDelete(pks []authority.PK) {
	for _, pk := range pks {
		p.deletes[string(pk.Encode())] = pk
	}
}

// snapshot returns a copy, so that it can be used outside the decider mutex.
func (p *pendingTxn) snapshot() *pendingTxn {
	s := newPendingTxn()
	for k, v := range p.puts {
		s.puts[k] = v
	}
	for k, v := range p.deletes {
		s.deletes[k] = v
	}
	return s
}

// changedSince reports whether p holds a different number of puts or deletes
// than snapshot. It is used to detect a body applied after a commit decision
// was made from snapshot.
func (p *pendingTxn) changedSince(snapshot *pendingTxn) bool {
	return len(p.puts) != len(snapshot.puts) || len(p.deletes) != len(snapshot.deletes)
}

// allPKs returns every key touched by the transaction, for locking.
func (p *pendingTxn) allPKs() []authority.PK {
	pks := make([]authority.PK, 0, len(p.puts)+len(p.deletes))
	for _, put := range p.puts {
		pks = append(pks, put.pk)
	}
	for k, pk := range p.deletes {
		if _, ok := p.puts[k]; !ok {
			pks = append(pks, pk)
		}
	}
	return pks
}

// companionCandidates returns the inserted keys that the transaction does not delete itself.
func (p *pendingTxn) companionCandidates() []authority.PK {
	pks := make([]authority.PK, 0, len(p.puts))
	for k, put := range p.puts {
		if _, ok := p.deletes[k]; !ok {
			pks = append(pks, put.pk)
		}
	}
	return pks
}
