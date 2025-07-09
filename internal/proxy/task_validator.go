package proxy

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// validator is a generic interface for validating tasks
type validator[T any] interface {
	validate(request T) error
}

// searchTaskValidator validates search tasks
type searchTaskValidator struct{}

var searchTaskValidatorInstance validator[*searchTask] = &searchTaskValidator{}

func (v *searchTaskValidator) validateSubSearch(subReq *internalpb.SubSearchRequest) error {
	maxResultEntries := paramtable.Get().ProxyCfg.MaxResultEntries.GetAsInt64()
	if maxResultEntries <= 0 {
		return nil
	}
	// check if number of result entries is too large
	nEntries := subReq.GetNq() * subReq.GetTopk()
	// if there is group size, multiply it
	if subReq.GetGroupSize() > 0 {
		nEntries *= subReq.GroupSize
	}
	if nEntries > maxResultEntries {
		return fmt.Errorf("number of result entries is too large")
	}
	return nil
}

func (v *searchTaskValidator) validateSearch(search *searchTask) error {
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
		return fmt.Errorf("number of result entries is too large")
	}
	return nil
}

func (v *searchTaskValidator) validate(search *searchTask) error {
	// if it is a hybrid search, check all sub-searches
	if search.SearchRequest.GetIsAdvanced() {
		for _, subReq := range search.SearchRequest.GetSubReqs() {
			if err := v.validateSubSearch(subReq); err != nil {
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
	case *searchTask:
		return searchTaskValidatorInstance.validate(t)
	default:
		return nil
	}
}
