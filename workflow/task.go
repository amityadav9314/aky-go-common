package workflow

import (
	"context"
	"fmt"
	"iter"
	"time"
)

// Task is the base contract every pipeline unit implements. On its own it only
// reports how failures are handled; the engine dispatches on whether a task
// also implements [SimpleTask] or [MultiValuedTask].
//
// Every task must explicitly define IsErrorFatal so authors choose whether
// errors should abort the chain or be treated as non-fatal.
type Task interface {
	// IsErrorFatal reports what happens when the task returns (or yields) an
	// error. When true, the error aborts the whole chain. When false, the error
	// is logged and the previous, unmodified state flows through as if the task
	// had never run.
	IsErrorFatal() bool
}

// SimpleTask receives one [State] and returns one [State]. It is the common
// case: read some values, compute, return a derived state via prev.ToBuilder().
//
// Return prev.Terminate()'s result to short-circuit the chain. Return an error
// to fail; whether that aborts the chain depends on [Task.IsErrorFatal].
type SimpleTask interface {
	Task
	Run(ctx context.Context, prev State) (State, error)
}

// MultiValuedTask emits zero or more States from a single input, fanning the
// pipeline out: every emitted state becomes an independent execution path for
// the remaining tasks. Use it for streaming sources (gRPC streams, SSE,
// paginated APIs, splitting a batch into items).
//
// The returned iterator follows the standard range-over-func error contract:
// on failure, yield (State{}, err) once and return; otherwise yield (state,
// nil) per item and stop early if yield returns false.
type MultiValuedTask interface {
	Task
	Stream(ctx context.Context, prev State) iter.Seq2[State, error]
}

// taskName returns a human-readable identifier for a task, used in error
// wrapping and time logging.
func taskName(t Task) string {
	return fmt.Sprintf("%T", t)
}

// ---------------------------------------------------------------------------
// Composable middleware (Go-native improvement over the Java/Python originals).
// Each wrapper returns a Task that preserves the wrapped task's flavor
// (SimpleTask vs MultiValuedTask) and its IsErrorFatal reporting.
// ---------------------------------------------------------------------------

// WithTimeout wraps task so each execution is bounded by d. The timeout applies
// per invocation: for a [MultiValuedTask] it bounds the entire emission stream.
// On timeout the task observes a cancelled context and its error surfaces
// according to the task's own [Task.IsErrorFatal].
//
// A non-positive d returns the task unchanged.
func WithTimeout(d time.Duration, task Task) Task {
	if d <= 0 {
		return task
	}
	switch t := task.(type) {
	case SimpleTask:
		return simpleFunc{
			fatal: t.IsErrorFatal(),
			name:  taskName(task),
			run: func(ctx context.Context, prev State) (State, error) {
				ctx, cancel := context.WithTimeout(ctx, d)
				defer cancel()
				return t.Run(ctx, prev)
			},
		}
	case MultiValuedTask:
		return multiFunc{
			fatal: t.IsErrorFatal(),
			name:  taskName(task),
			stream: func(ctx context.Context, prev State) iter.Seq2[State, error] {
				return func(yield func(State, error) bool) {
					ctx, cancel := context.WithTimeout(ctx, d)
					defer cancel()
					for s, err := range t.Stream(ctx, prev) {
						if !yield(s, err) {
							return
						}
					}
				}
			},
		}
	default:
		return task
	}
}

// WithRetry wraps a [SimpleTask] so a failing Run is retried up to attempts
// total tries (the initial call counts as the first). The context is honored
// between attempts: a cancelled context stops retrying immediately.
//
// Retry only applies to SimpleTask, where re-running from the same input is
// well defined. For any other task type, or attempts <= 1, the task is returned
// unchanged (retrying a partially consumed multi-valued stream is not safe in
// general).
func WithRetry(attempts int, task Task) Task {
	st, ok := task.(SimpleTask)
	if !ok || attempts <= 1 {
		return task
	}
	return simpleFunc{
		fatal: task.IsErrorFatal(),
		name:  taskName(task),
		run: func(ctx context.Context, prev State) (State, error) {
			var lastErr error
			for attempt := 0; attempt < attempts; attempt++ {
				if err := ctx.Err(); err != nil {
					return State{}, err
				}
				state, err := st.Run(ctx, prev)
				if err == nil {
					return state, nil
				}
				lastErr = err
			}
			return State{}, fmt.Errorf("workflow: task %s failed after %d attempts: %w", taskName(task), attempts, lastErr)
		},
	}
}

// simpleFunc adapts a closure into a [SimpleTask]; used by the middleware
// wrappers and available for inline tasks.
type simpleFunc struct {
	fatal bool
	name  string
	run   func(ctx context.Context, prev State) (State, error)
}

func (s simpleFunc) IsErrorFatal() bool                                 { return s.fatal }
func (s simpleFunc) Run(ctx context.Context, prev State) (State, error) { return s.run(ctx, prev) }

// multiFunc adapts a closure into a [MultiValuedTask].
type multiFunc struct {
	fatal  bool
	name   string
	stream func(ctx context.Context, prev State) iter.Seq2[State, error]
}

func (m multiFunc) IsErrorFatal() bool { return m.fatal }
func (m multiFunc) Stream(ctx context.Context, prev State) iter.Seq2[State, error] {
	return m.stream(ctx, prev)
}
