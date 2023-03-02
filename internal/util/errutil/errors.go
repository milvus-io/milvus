package errutil

import (
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
)

type multiErrors struct {
	errs []error
}

func (e multiErrors) Unwrap() error {
	if len(e.errs) <= 1 {
		return nil
	}
	return multiErrors{
		errs: e.errs[1:],
	}
}

func (e multiErrors) Error() string {
	final := e.errs[0]
	for i := 1; i < len(e.errs); i++ {
		final = errors.Wrap(e.errs[i], final.Error())
	}
	return final.Error()
}

func (e multiErrors) Is(err error) bool {
	for _, item := range e.errs {
		if errors.Is(item, err) {
			return true
		}
	}
	return false
}

func Combine(errs ...error) error {
	errs = lo.Filter(errs, func(err error, _ int) bool { return err != nil })
	if len(errs) == 0 {
		return nil
	}
	return multiErrors{
		errs,
	}
}
