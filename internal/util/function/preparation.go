package function

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type deferredPreparationKey struct{}

// WithDeferredPreparation makes materialization lease only ready runners.
// Callers must handle PreparationRequiredError outside their mutation boundary,
// then retry materialization to resolve the current schema again.
func WithDeferredPreparation(ctx context.Context) context.Context {
	return context.WithValue(ctx, deferredPreparationKey{}, true)
}

// PreparationRequiredError suspends materialization before any fields change.
// It carries the exact runner that needs initialization. It is consumed by the
// redo interceptor and must not be converted to a terminal append error.
type PreparationRequiredError struct {
	entry *functionRunnerEntry
}

func (e *PreparationRequiredError) Error() string {
	return "function runner requires preparation outside WAL append interceptors"
}

func (e *PreparationRequiredError) Unwrap() error {
	return merr.ErrServiceUnavailable
}

// Prepare joins the existing initialization flight with this caller's context.
// Schema GC may remove the entry while preparation is pending; in that case the
// next append attempt must resolve the current schema instead of using this entry.
func (e *PreparationRequiredError) Prepare(ctx context.Context) error {
	err := e.entry.ensureRunner(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, errFunctionRunnerEntryRemoved) {
		return nil
	}
	return err
}

func (e *functionRunnerEntry) getReadyRunner() (FunctionRunner, func(), error) {
	e.mu.RLock()
	if e.isReadyLocked() {
		return e.runner, e.mu.RUnlock, nil
	}
	e.mu.RUnlock()
	return nil, nil, &PreparationRequiredError{entry: e}
}
