package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/amityadav9314/aky-go-common/logger"
)

// ---------------------------------------------------------------------------
// Interface
// ---------------------------------------------------------------------------

type HttpClient interface {
	Do(ctx context.Context, opts *RequestOptions) (*Response, error)
	DoStream(ctx context.Context, opts *RequestOptions) (io.ReadCloser, http.Header, error)
}

// ---------------------------------------------------------------------------
// Request options (per-call configuration)
// ---------------------------------------------------------------------------

type RequestOptions struct {
	Method         string
	URL            string
	Headers        map[string]string
	Body           any
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	LogReqRes      bool
	ResponseModel  any
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

type Response struct {
	StatusCode int
	Body       []byte
	Header     http.Header
	Parsed     any
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

type TimeoutError struct {
	Method string
	URL    string
	Err    error
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("httpclient: timeout %s %s: %v", e.Method, e.URL, e.Err)
}

func (e *TimeoutError) Unwrap() error { return e.Err }

type StatusError struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("httpclient: %s %s returned status %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

type RequestError struct {
	Method string
	URL    string
	Err    error
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("httpclient: request error %s %s: %v", e.Method, e.URL, e.Err)
}

func (e *RequestError) Unwrap() error { return e.Err }

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

type httpClientImpl struct {
	client *http.Client
}

func newHttpClient() *httpClientImpl {
	return &httpClientImpl{
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (h *httpClientImpl) Do(ctx context.Context, opts *RequestOptions) (*Response, error) {
	req, _, err := h.prepareRequest(ctx, opts)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := h.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		if isTimeout(err) {
			return nil, &TimeoutError{Method: opts.Method, URL: opts.URL, Err: err}
		}
		return nil, &RequestError{Method: opts.Method, URL: opts.URL, Err: err}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &RequestError{Method: opts.Method, URL: opts.URL, Err: err}
	}

	if opts.LogReqRes {
		logger.Info(ctx, fmt.Sprintf("<-- %s %s | status=%d | latency=%s", opts.Method, opts.URL, resp.StatusCode, latency), nil, string(respBody))
	}

	if resp.StatusCode >= 400 {
		return nil, &StatusError{
			Method:     opts.Method,
			URL:        opts.URL,
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
		}
	}

	result := &Response{
		StatusCode: resp.StatusCode,
		Body:       respBody,
		Header:     resp.Header,
	}

	if opts.ResponseModel != nil {
		if err := json.Unmarshal(respBody, opts.ResponseModel); err != nil {
			return result, &RequestError{Method: opts.Method, URL: opts.URL, Err: fmt.Errorf("failed to parse response: %w", err)}
		}
		result.Parsed = opts.ResponseModel
	}

	return result, nil
}

func (h *httpClientImpl) DoStream(ctx context.Context, opts *RequestOptions) (io.ReadCloser, http.Header, error) {
	req, _, err := h.prepareRequest(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		if isTimeout(err) {
			return nil, nil, &TimeoutError{Method: opts.Method, URL: opts.URL, Err: err}
		}
		return nil, nil, &RequestError{Method: opts.Method, URL: opts.URL, Err: err}
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, nil, &StatusError{
			Method:     opts.Method,
			URL:        opts.URL,
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	return resp.Body, resp.Header, nil
}

func (h *httpClientImpl) prepareRequest(ctx context.Context, opts *RequestOptions) (*http.Request, []byte, error) {
	var bodyBytes []byte
	if opts.Body != nil {
		var err error
		bodyBytes, err = json.Marshal(opts.Body)
		if err != nil {
			return nil, nil, &RequestError{Method: opts.Method, URL: opts.URL, Err: err}
		}
	}

	req, err := http.NewRequestWithContext(ctx, opts.Method, opts.URL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, nil, &RequestError{Method: opts.Method, URL: opts.URL, Err: err}
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range opts.Headers {
		req.Header.Set(k, v)
	}

	if opts.LogReqRes {
		logger.Info(ctx, fmt.Sprintf("--> %s %s", opts.Method, opts.URL), bodyBytes, nil)
	}

	return req, bodyBytes, nil
}

func isTimeout(err error) bool {
	type timeout interface {
		Timeout() bool
	}
	if t, ok := err.(timeout); ok {
		return t.Timeout()
	}
	return false
}

// ---------------------------------------------------------------------------
// Factory (singleton)
// ---------------------------------------------------------------------------

var (
	singleton HttpClient
	once      sync.Once
)

func GetClient() HttpClient {
	once.Do(func() {
		singleton = newHttpClient()
	})
	return singleton
}
