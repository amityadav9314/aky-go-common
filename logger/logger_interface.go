package logger

import (
	"context"
	"time"
)

// LogLevel represents the severity level of a log entry.
type LogLevel uint8

const (
	LvlDebug LogLevel = iota
	LvlInfo
	LvlWarn
	LvlError
)

type fieldKind uint8

const (
	fieldString fieldKind = iota
	fieldError
	fieldAny
)

// LoggingField represents a structured logging field with a specific type.
type LoggingField struct {
	key   FieldKey
	kind  fieldKind
	value any
}

type MetricType uint8

const (
	LogFile MetricType = iota
	LogCounter
)

type LogLevelField string

const (
	LogLevelInfo  LogLevelField = "INFO"
	LogLevelWarn  LogLevelField = "WARN"
	LogLevelDebug LogLevelField = "DEBUG"
	LogLevelError LogLevelField = "ERROR"
)

// BFFLogger MetricType determines optional counter emission (see metric package).
// LogFile -> application log only
// LogCounter -> application log + increment metric counter on errors
type BFFLogger interface {
	Debug(ctx context.Context, msg, servNm string, request any, response any, timeTaken time.Duration, metricType []MetricType, fields ...LoggingField)
	InfoMsg(ctx context.Context, msg, servNm string, metricType []MetricType, fields ...LoggingField)
	Info(ctx context.Context, msg, servNm string, request any, response any, timeTaken time.Duration, metricType []MetricType, fields ...LoggingField)
	ErrorMsg(ctx context.Context, msg, servNm string, e error, metricType []MetricType, fields ...LoggingField)
	Error(ctx context.Context, msg, servNm string, request any, response any, timeTaken time.Duration, e error, metricType []MetricType, fields ...LoggingField)
	Flush()
}

// FieldAny attaches an arbitrary value under a vocabulary key.
func FieldAny(key FieldKey, val any) LoggingField {
	return LoggingField{key: key, kind: fieldAny, value: val}
}

// FieldString attaches a string value under a vocabulary key.
func FieldString(key FieldKey, val string) LoggingField {
	return LoggingField{key: key, kind: fieldString, value: val}
}

// FieldError attaches an error value.
func FieldError(val error) LoggingField {
	return LoggingField{kind: fieldError, value: val}
}
