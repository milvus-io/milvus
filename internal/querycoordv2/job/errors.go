package job

import "errors"

var (
	// Common errors
	ErrInvalidRequest = errors.New("InvalidRequest")

	// Load errors
	ErrCollectionLoaded        = errors.New("CollectionLoaded")
	ErrLoadParameterMismatched = errors.New("LoadParameterMismatched")
)
