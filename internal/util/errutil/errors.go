package errutil

import "github.com/cockroachdb/errors"

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
	return errors.IsAny(err, e.errs...)
}

func Combine(errs ...error) error {
	if len(errs) == 0 {
		return nil
	}
	return multiErrors{
		errs,
	}
}
