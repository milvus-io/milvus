//go:build test

package extension

// ResetForTest clears the installed provider so a test can install another.
//
// It is built only under the "test" tag and therefore does not exist in a
// production binary. That matters: clearing the provider at runtime would drop
// every consumer back to the native path, which for a form that declared a
// capability as required means running without it - the exact failure
// SetProvider's requirement check exists to prevent.
func ResetForTest() { installed.Store(nil) }
