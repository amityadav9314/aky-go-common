package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

const requestIDKey = "crId"

type JsonLog struct {
	Msg        string `json:"msg"`
	CrId       string `json:"crId"`
	Req        string `json:"req"`
	Res        string `json:"res"`
	MetricName string `json:"metricName,omitempty"`
}

func Info(ctx context.Context, msg string, request any, response any) {
	logMessage(ctx, msg, "", request, response)
}

func Error(ctx context.Context, msg string, metricName string, request any, response any) {
	logMessage(ctx, msg, metricName, request, response)
}

type backgroundLogger struct{}

func GetBackgroundLogger() BFFLogger {
	return backgroundLogger{}
}

func (backgroundLogger) Debug(ctx context.Context, msg, servNm string, request any, response any, timeTaken time.Duration, metricType []MetricType, fields ...LoggingField) {
	logMessage(ctx, msg, "", logPayload(servNm, request, timeTaken, metricType, fields), response)
}

func (backgroundLogger) InfoMsg(ctx context.Context, msg, servNm string, metricType []MetricType, fields ...LoggingField) {
	logMessage(ctx, msg, "", logPayload(servNm, nil, 0, metricType, fields), nil)
}

func (backgroundLogger) Info(ctx context.Context, msg, servNm string, request any, response any, timeTaken time.Duration, metricType []MetricType, fields ...LoggingField) {
	logMessage(ctx, msg, "", logPayload(servNm, request, timeTaken, metricType, fields), response)
}

func (backgroundLogger) ErrorMsg(ctx context.Context, msg, servNm string, e error, metricType []MetricType, fields ...LoggingField) {
	logMessage(ctx, msg, "", logPayload(servNm, nil, 0, metricType, append(fields, FieldError(e))), nil)
}

func (backgroundLogger) Error(ctx context.Context, msg, servNm string, request any, response any, timeTaken time.Duration, e error, metricType []MetricType, fields ...LoggingField) {
	logMessage(ctx, msg, "", logPayload(servNm, request, timeTaken, metricType, append(fields, FieldError(e))), response)
}

func (backgroundLogger) Flush() {}

func logPayload(servNm string, request any, timeTaken time.Duration, metricType []MetricType, fields []LoggingField) map[string]any {
	payload := make(map[string]any, 4)
	if servNm != "" {
		payload[FieldKeyServNm.String()] = servNm
	}
	if request != nil {
		payload[FieldKeyRequest.String()] = request
	}
	if timeTaken > 0 {
		payload[FieldKeyTimeTaken.String()] = timeTaken.String()
	}
	if len(metricType) > 0 {
		payload["metricType"] = metricType
	}
	for key, value := range loggingFieldsToMap(fields) {
		payload[key] = value
	}
	return payload
}

func loggingFieldsToMap(fields []LoggingField) map[string]any {
	out := make(map[string]any, len(fields))
	for _, field := range fields {
		switch field.kind {
		case fieldString, fieldAny:
			out[field.key.String()] = field.value
		case fieldError:
			if err, ok := field.value.(error); ok && err != nil {
				out["error"] = err.Error()
			}
		}
	}
	return out
}

func logMessage(ctx context.Context, msg string, metricName string, request any, response any) {
	crId, _ := ctx.Value(requestIDKey).(string)
	reqStr := toString(request)
	resStr := toString(response)
	jsonLog := JsonLog{
		CrId:       crId,
		Req:        reqStr,
		Res:        resStr,
		Msg:        msg,
		MetricName: metricName,
	}
	jsonByte, err := json.Marshal(jsonLog)
	if err != nil {
		panic("Can not convert to json for logging. Error: " + string(err.Error()))
	}

	log.Println(string(jsonByte))
}

func toString(v any) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {

	case string:
		return val

	case []byte:
		return string(val)

	case *http.Response:
		// don’t try to marshal whole response
		return fmt.Sprintf("status=%s", val.Status)

	default:
		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(b)
	}
}
