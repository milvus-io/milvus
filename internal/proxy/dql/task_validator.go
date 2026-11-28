package dql

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// taskValidator is a generic interface for validating tasks.
type taskValidator[T any] interface {
	validate(request T) error
}

// searchTaskValidator validates search tasks
type searchTaskValidator struct{}

var searchTaskValidatorInstance taskValidator[*SearchTask] = &searchTaskValidator{}

// validateSubSearch checks the result size of one hybrid sub-search. groupSize
// is the group size the user requested, not the larger size the sub-search
// keeps internally (see rankParams.GetSubSearchGroupSize), so enlarging the
// per-group buffer does not reject requests that passed before.
func (v *searchTaskValidator) validateSubSearch(subReq *internalpb.SubSearchRequest, groupSize int64) error {
	maxResultEntries := paramtable.Get().ProxyCfg.MaxResultEntries.GetAsInt64()
	if maxResultEntries <= 0 {
		return nil
	}
	// check if number of result entries is too large
	nEntries := subReq.GetNq() * subReq.GetTopk()
	// if there is group size, multiply it
	if groupSize > 0 {
		nEntries *= groupSize
	}
	if nEntries > maxResultEntries {
		return merr.WrapErrParameterInvalidMsg("number of result entries is too large")
	}
	return nil
}

func (v *searchTaskValidator) validateSearch(search *SearchTask) error {
	maxResultEntries := paramtable.Get().ProxyCfg.MaxResultEntries.GetAsInt64()
	if maxResultEntries <= 0 {
		return nil
	}
	// check if number of result entries is too large
	nEntries := search.GetNq() * search.GetTopk()
	// if there is group size, multiply it
	if search.GetGroupSize() > 0 {
		nEntries *= search.GroupSize
	}
	if nEntries > maxResultEntries {
		return merr.WrapErrParameterInvalidMsg("number of result entries is too large")
	}
	return nil
}

func (v *searchTaskValidator) validate(search *SearchTask) error {
	// if it is a hybrid search, check all sub-searches
	if search.GetIsAdvanced() {
		for _, subReq := range search.GetSubReqs() {
			groupSize := subReq.GetGroupSize()
			if search.rankParams != nil {
				groupSize = search.rankParams.GetGroupSize()
			}
			if err := v.validateSubSearch(subReq, groupSize); err != nil {
				return err
			}
		}
	} else {
		if err := v.validateSearch(search); err != nil {
			return err
		}
	}
	return nil
}

func ValidateTask(task any) error {
	switch t := task.(type) {
	case *SearchTask:
		return searchTaskValidatorInstance.validate(t)
	default:
		return nil
	}
}
