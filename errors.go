package sazanami

import "errors"

type fatalMarker interface{ fatal() bool }

type fatalError struct{ err error }

func (f fatalError) Error() string { return f.err.Error() }
func (f fatalError) Unwrap() error { return f.err }
func (fatalError) fatal() bool     { return true }

// Abort marks an error as fatal for the stage, bypassing item-level policies.
func Abort(err error) error {
	if err == nil {
		return nil
	}
	return fatalError{err: err}
}

func isFatalError(err error) bool {
	if err == nil {
		return false
	}
	var marker fatalMarker
	return errors.As(err, &marker)
}
