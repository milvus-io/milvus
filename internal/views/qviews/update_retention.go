package qviews

// ReadTimestamps counts physical timestamp requirements under the owner's mutex.
// A zero timestamp is a real protection (including an uncertified legacy View).
type ReadTimestamps map[uint64]int

func (s *ReadTimestamps) Add(ts uint64) {
	if *s == nil {
		*s = make(ReadTimestamps)
	}
	(*s)[ts]++
}

func (s ReadTimestamps) Remove(ts uint64) {
	if s[ts] == 1 {
		delete(s, ts)
	} else {
		s[ts]--
	}
}

func (s ReadTimestamps) Limit(cutoff uint64) uint64 {
	for ts := range s {
		cutoff = min(cutoff, ts)
	}
	return cutoff
}

// UpdateRetention belongs to one physical update record. All methods run
// under the same mutex as View admission and read acquisition.
type UpdateRetention struct {
	Reads     ReadTimestamps
	Reserved  uint64
	completed uint64
	running   bool
}

// Reserve excludes all earlier admissions before the caller starts native GC.
// An overlapping acquisition may advance the target of the existing worker.
func (s *UpdateRetention) Reserve(cutoff uint64) bool {
	cutoff = s.Reads.Limit(cutoff)
	if cutoff < s.Reserved || cutoff <= s.completed {
		return false
	}
	s.Reserved = cutoff
	if s.running {
		return false
	}
	s.running = true
	return true
}

// Finish keeps the admission fence even if native reclamation failed.
func (s *UpdateRetention) Finish(target uint64, err error) (next uint64, again bool) {
	if err == nil {
		s.completed = target
		if target < s.Reserved {
			return s.Reserved, true
		}
	}
	s.running = false
	return 0, false
}
