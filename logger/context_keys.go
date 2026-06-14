package logger

// ContextKey is the typed key used to carry logging-relevant values on a
// context.Context. It is owned by the logger so the library stays free of any
// application package dependency; transport code sets these keys and the logger
// reads them during field enrichment.
type ContextKey int

const (
	ContextKeyRequestID ContextKey = iota
	ContextKeyAPIName
	ContextKeySourceCity
	ContextKeyDestination
	ContextKeyExtraInfo
	ContextKeyEnv
	ContextKeyCaller
	ContextKeyFileName
)
