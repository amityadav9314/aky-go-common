package workflow

import (
	"context"
	"fmt"
	"iter"
)

// Stream is the engine's stream type: a pull-based, lazy sequence of states
// where each step may carry an error as its second value.
//
// It follows the standard Go range-over-func error contract: a producer yields
// (State{}, err) exactly once on failure and then returns; a consumer must stop
// iterating (break or return false) as soon as it observes a non-nil error.
//
// Most callers should not consume a Stream directly. Prefer the safe helpers
// on [ExecutionChain] ([ExecutionChain.Execute], [ExecutionChain.Collect],
// [ExecutionChain.First]) which surface a plain Go error.
type Stream = iter.Seq2[State, error]

// stage is an internal pipeline step: given a context it produces a Stream.
// Wrapping production in a func(ctx) keeps the whole chain lazy and re-runnable,
// and lets every level receive cancellation.
type stage func(ctx context.Context) Stream

// collect drains the stream into a slice, stopping at the first error.
func collect(seq Stream) ([]State, error) {
	var out []State
	for s, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// first returns the first emitted state, or an error if the stream fails or is
// empty.
func first(seq Stream) (State, error) {
	for s, err := range seq {
		if err != nil {
			return State{}, err
		}
		return s, nil
	}
	return State{}, fmt.Errorf("workflow: stream emitted no values")
}

// single returns the only emitted state. It errors if the stream fails, is
// empty, or emits more than once (which happens when the chain fans out via a
// MultiValuedTask but the caller expected a single result).
func single(seq Stream) (State, error) {
	states, err := collect(seq)
	if err != nil {
		return State{}, err
	}
	if len(states) != 1 {
		return State{}, fmt.Errorf("workflow: expected exactly one result, got %d", len(states))
	}
	return states[0], nil
}

// emit is a helper for producers: it yields a fatal error on the stream and
// reports whether the consumer wants more (always false after an error, by
// contract). Producers should return immediately when emitErr returns.
func emitErr(yield func(State, error) bool, err error) {
	yield(State{}, err)
}
