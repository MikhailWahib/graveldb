// Package errors provides custom error types for consistent error handling.
package errors

import (
	"errors"
	"fmt"
)

// Code represents an error code.
type Code string

const (
	// ErrCodeIO indicates an I/O error.
	ErrCodeIO Code = "IO"
	// ErrCodeNotFound indicates a resource was not found.
	ErrCodeNotFound Code = "NOT_FOUND"
	// ErrCodeCorruption indicates data corruption.
	ErrCodeCorruption Code = "CORRUPTION"
	// ErrCodeClosed indicates a resource is closed.
	ErrCodeClosed Code = "CLOSED"
	// ErrCodeInternal indicates an internal error.
	ErrCodeInternal Code = "INTERNAL"
)

// ErrNotFound represents a Not Found error
var ErrNotFound = &Error{Code: ErrCodeNotFound}

// Error represents a custom error with code, message, and underlying error.
type Error struct {
	Code    Code
	Message string
	Err     error
}

// Error returns the error message.
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s", e.Code)
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	return e.Err
}

// Is reports whether this error matches target.
func (e *Error) Is(target error) bool {
	if t, ok := target.(*Error); ok {
		return e.Code == t.Code
	}
	return errors.Is(e.Err, target)
}

// IO creates an I/O error.
func IO(msg string, err error) error {
	return &Error{Code: ErrCodeIO, Message: msg, Err: err}
}

// Corruption creates a corruption error.
func Corruption(msg string, err error) error {
	return &Error{Code: ErrCodeCorruption, Message: msg, Err: err}
}

// Closed creates a closed error.
func Closed(msg string, err error) error {
	return &Error{Code: ErrCodeClosed, Message: msg, Err: err}
}

// Internal creates an internal error.
func Internal(msg string, err error) error {
	return &Error{Code: ErrCodeInternal, Message: msg, Err: err}
}
