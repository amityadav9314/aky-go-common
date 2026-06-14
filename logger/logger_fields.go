package logger

// FieldKey is a structured-log field key drawn from a closed vocabulary.
//
// The underlying name is unexported on purpose: callers cannot fabricate keys
// from arbitrary string literals, they must use one of the exported FieldKey
// values declared below. Introducing a new log field therefore requires adding
// a constant here, which keeps the log JSON from sprawling over time.
type FieldKey struct{ name string }

// String returns the underlying field name. It doubles as the context-value key
// for the fields that are also propagated through context.
func (k FieldKey) String() string { return k.name }

// The closed field vocabulary. Add a value here to introduce a new log field.
var (
	FieldKeyRequest           = FieldKey{"request"}
	FieldKeyResponse          = FieldKey{"response"}
	FieldKeyAPIName           = FieldKey{"apiName"}
	FieldKeyRequestID         = FieldKey{"requestId"}
	FieldKeyStackTrace        = FieldKey{"stackTrace"}
	FieldKeyException         = FieldKey{"exception"}
	FieldKeyFileName          = FieldKey{"fileName"}
	FieldKeyTimeStamp         = FieldKey{"timeStamp"}
	FieldKeyTimeTaken         = FieldKey{"timeTaken"}
	FieldKeyEnv               = FieldKey{"env"}
	FieldKeyDeploymentVersion = FieldKey{"deployment_version"}
	FieldKeyServNm            = FieldKey{"servNm"}
	FieldKeyLevel             = FieldKey{"level"}
	FieldKeyExtraInfo         = FieldKey{"extraInfo"}
	FieldKeyCaller            = FieldKey{"caller"}
	FieldKeySourceCity        = FieldKey{"source_city_name"}
	FieldKeyDestination       = FieldKey{"destination_location"}

	// Workflow engine field keys.
	FieldKeyTask    = FieldKey{"task"}
	FieldKeyLogID   = FieldKey{"log_id"}
	FieldKeyElapsed = FieldKey{"elapsed"}
)
