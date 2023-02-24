package mqwrapper

import "github.com/cockroachdb/errors"

// ErrTopicNotExist topic not exist error.
var ErrTopicNotExist = errors.New("topic not exist")
