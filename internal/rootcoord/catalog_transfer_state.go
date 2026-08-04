package rootcoord

import (
	"errors"
	"sync"
	"time"
)

var (
	errCollectionTransferring          = errors.New("collection is transferring")
	errCollectionTransferredOut        = errors.New("collection is transferred out")
	errCollectionHasInFlightOperations = errors.New("collection has in-flight operations")
	errStaleTransferEpoch              = errors.New("stale transfer epoch")
	errTransferMismatch                = errors.New("transfer id mismatch")
	errTransferStateMismatch           = errors.New("transfer state mismatch")
	errTransferIDRequired              = errors.New("transfer id is required")
	errTransferEpochRequired           = errors.New("transfer epoch is required")
)

type transferCollectionState string

const (
	transferStateTransferringOut transferCollectionState = "TRANSFERRING_OUT"
	transferStateTransferredOut  transferCollectionState = "TRANSFERRED_OUT"
)

type transferGateEntry struct {
	transferID string
	epoch      int64
	state      transferCollectionState
}

type transferGate struct {
	mu          sync.Mutex
	cond        *sync.Cond
	collections map[int64]transferGateEntry
	inFlight    map[int64]int
}

func newTransferGate() *transferGate {
	g := &transferGate{
		collections: make(map[int64]transferGateEntry),
		inFlight:    make(map[int64]int),
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *transferGate) Freeze(collectionID int64, transferID string, epoch int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inFlight[collectionID] > 0 {
		return errCollectionHasInFlightOperations
	}
	return g.freezeLocked(collectionID, transferID, epoch)
}

func (g *transferGate) FreezeWithDrain(collectionID int64, transferID string, epoch int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	g.mu.Lock()
	defer g.mu.Unlock()

	_, existed := g.collections[collectionID]
	if err := g.freezeLocked(collectionID, transferID, epoch); err != nil {
		return err
	}
	for g.inFlight[collectionID] > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if !existed {
				if current, ok := g.collections[collectionID]; ok &&
					current.transferID == transferID &&
					current.epoch == epoch &&
					current.state == transferStateTransferringOut {
					delete(g.collections, collectionID)
					g.cond.Broadcast()
				}
			}
			return errCollectionHasInFlightOperations
		}
		timer := time.AfterFunc(remaining, func() {
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		})
		g.cond.Wait()
		timer.Stop()
	}
	return nil
}

func (g *transferGate) freezeLocked(collectionID int64, transferID string, epoch int64) error {
	if transferID == "" {
		return errTransferIDRequired
	}
	if epoch <= 0 {
		return errTransferEpochRequired
	}
	current, ok := g.collections[collectionID]
	if ok {
		if current.transferID == transferID && current.epoch == epoch && current.state == transferStateTransferringOut {
			return nil
		}
		return errCollectionTransferring
	}
	g.collections[collectionID] = transferGateEntry{
		transferID: transferID,
		epoch:      epoch,
		state:      transferStateTransferringOut,
	}
	return nil
}

func (g *transferGate) AllowUserOperation(collectionID int64, epoch int64) error {
	done, err := g.BeginUserOperation(collectionID, epoch)
	if err != nil {
		return err
	}
	done()
	return nil
}

func (g *transferGate) BeginUserOperation(collectionID int64, epoch int64) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.collections[collectionID]
	if !ok {
		g.inFlight[collectionID]++
		return g.endUserOperation(collectionID), nil
	}
	if epoch != 0 && epoch < current.epoch {
		return nil, errStaleTransferEpoch
	}
	switch current.state {
	case transferStateTransferringOut:
		return nil, errCollectionTransferring
	case transferStateTransferredOut:
		return nil, errCollectionTransferredOut
	default:
		g.inFlight[collectionID]++
		return g.endUserOperation(collectionID), nil
	}
}

func (g *transferGate) endUserOperation(collectionID int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.inFlight[collectionID] > 0 {
				g.inFlight[collectionID]--
			}
			g.cond.Broadcast()
		})
	}
}

func (g *transferGate) AllowTransferOperation(collectionID int64, transferID string, epoch int64, expected transferCollectionState) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.collections[collectionID]
	if !ok {
		return errTransferMismatch
	}
	if current.transferID != transferID {
		return errTransferMismatch
	}
	if epoch != current.epoch {
		return errStaleTransferEpoch
	}
	if current.state != expected {
		return errTransferStateMismatch
	}
	return nil
}

func (g *transferGate) Deactivate(collectionID int64, transferID string, epoch int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.collections[collectionID]
	if !ok {
		return nil
	}
	if current.transferID != transferID {
		return errTransferMismatch
	}
	if current.epoch != epoch {
		return errStaleTransferEpoch
	}
	if current.state != transferStateTransferringOut && current.state != transferStateTransferredOut {
		return errTransferStateMismatch
	}
	current.state = transferStateTransferredOut
	g.collections[collectionID] = current
	return nil
}

func (g *transferGate) Abort(collectionID int64, transferID string, epoch int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.collections[collectionID]
	if !ok {
		return nil
	}
	if current.transferID != transferID {
		return errTransferMismatch
	}
	if current.epoch != epoch {
		return errStaleTransferEpoch
	}
	if current.state != transferStateTransferringOut {
		return errTransferStateMismatch
	}
	delete(g.collections, collectionID)
	g.cond.Broadcast()
	return nil
}

func (g *transferGate) Restore(collectionID int64, entry transferGateEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.collections[collectionID] = entry
	g.cond.Broadcast()
}
