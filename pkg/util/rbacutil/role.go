package rbacutil

import "github.com/milvus-io/milvus/pkg/v2/util/merr"

func ValidateRoleDescription(description string, maxLength int) error {
	if len(description) > maxLength {
		return merr.WrapErrParameterInvalidRange(0,
			maxLength,
			len(description),
			"the length of role description must be not greater than limit")
	}
	return nil
}
