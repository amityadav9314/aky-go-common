// Package apperror provides structured application errors and metric labels.
//
// It is catalog-agnostic: callers supply error metadata via [Meta] (typically
// derived from their own error catalog) through [New], and configure the
// fallback used by [Ensure] once at startup via [Configure].
package apperror

import (
	"fmt"
)

// Error is a structured error with HTTP status and catalog metadata.
type Error struct {
	Code        string
	Message     string
	Description string
	HTTPStatus  int
	Origin      string
	Owner       string
	Cause       error
}

// Meta is the metadata required to build an [Error]. Callers map their own
// error catalog entries into this shape.
type Meta struct {
	Code        string
	Description string
	HTTPStatus  int
	Origin      string
	Owner       string
}

// defaultUnknown is the fallback metadata used by [Ensure] when it cannot
// derive a structured error. Set it once at startup with [Configure].
var defaultUnknown Meta

// Configure sets the fallback metadata used by [Ensure]. Call it once during
// application bootstrap with the catalog's "unknown error" entry.
func Configure(unknown Meta) {
	defaultUnknown = unknown
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// New builds an [Error] from meta, optionally wrapping cause.
func New(meta Meta, cause error) *Error {
	return &Error{
		Code:        meta.Code,
		Message:     meta.Description,
		Description: meta.Description,
		HTTPStatus:  meta.HTTPStatus,
		Origin:      meta.Origin,
		Owner:       meta.Owner,
		Cause:       cause,
	}
}

// Ensure guarantees a structured *Error for upstream handling. A valid appError
// is returned unchanged; otherwise a fallback is built from the metadata
// registered via [Configure], wrapping cause when present.
func Ensure(appError *Error, cause error) *Error {
	if appError != nil && appError.HTTPStatus != 0 && appError.Code != "" {
		return appError
	}
	fallback := New(defaultUnknown, cause)
	if cause != nil {
		fallback.Description = fmt.Sprintf("%s, Msg: %v", fallback.Description, cause)
	}
	return fallback
}

// MetricName builds a metric/log label: apiName-httpStatus-errorCode-origin-owner.
// When appError is nil, only apiName is returned (success path).
func MetricName(apiName string, appError *Error) string {
	if appError == nil {
		return apiName
	}
	return fmt.Sprintf("%s-%d-%s-%s-%s", apiName, appError.HTTPStatus, appError.Code, appError.Origin, appError.Owner)
}

// MetricNameForStatus builds a metric label when only HTTP status is known (e.g. middleware).
func MetricNameForStatus(apiName string, statusCode int) string {
	if statusCode < 400 {
		return apiName
	}
	return fmt.Sprintf("%s-%d", apiName, statusCode)
}
