// Package workflow is a declarative task-pipeline engine.
//
// It is the Go counterpart of the Java (RxJava) and Python (asyncio)
// "ExecutionChain" workflow libraries. A pipeline is built declaratively with
// [Define] / [ExecutionChain.Next], executed with a [context.Context], and
// produces a stream of [State] values.
//
// # Core concepts
//
//   - [State] is an immutable bag of key->value results passed between tasks.
//     Reads and writes are strongly typed through [StateKey].
//   - [Task] is the base contract; it only reports whether a failure is fatal.
//   - [SimpleTask] takes one State and returns one State.
//   - [MultiValuedTask] emits many States, fanning the pipeline out so each
//     emission becomes its own execution path.
//   - [ExecutionChain] wires tasks into levels. A level with one task runs
//     sequentially; a level with several tasks runs them in parallel and can
//     fan back in with a [ZipperFunc].
//
// # Typed state access
//
// Go methods cannot have their own type parameters, so typed access to a State
// is provided through package-level generic functions ([GetValue],
// [GetRequiredValue], [SetValue]) keyed by a [StateKey] rather than through methods. This
// keeps writes compile-time checked: SetValue(b, countKey, "oops") will not compile
// when countKey is a StateKey[int].
//
// See the package examples for end-to-end usage.
package workflow

import "fmt"

// terminatedKey marks a State as terminated. It lives in the same map as user
// values but uses a name that cannot collide with a typed StateKey created via
// NewKey, because callers never construct a StateKey with this exact name for
// their own data (and even if they did, the value type bool is what we use).
const terminatedKey = "__workflow_terminated__"

// StateKey is a strongly-typed handle for a value stored in a [State]. The type
// parameter T is the type of the value associated with the key; it makes reads
// and writes through [GetValue], [GetRequiredValue] and [SetValue] type-safe.
//
// Create keys once, typically as package-level variables:
//
//	var Count = workflow.NewKey[int]("count")
type StateKey[T any] struct {
	// Name is the underlying map key. Two StateKeys with the same Name refer
	// to the same slot regardless of their type parameter, so keep names
	// unique per logical value.
	Name string
}

// NewKey creates a typed [StateKey] with the given name.
func NewKey[T any](name string) StateKey[T] {
	return StateKey[T]{Name: name}
}

// State is an immutable container of results accumulated by previous tasks.
//
// The zero value (State{}) is a valid, empty state: reads return the zero value
// of their type and ToBuilder starts from an empty map. Prefer [NewState] for
// clarity when constructing an explicit empty state.
//
// State is immutable: every mutation goes through a [Builder] and produces a
// new State. Reads via [State.Values] return a defensive copy so the internal
// map can never be mutated by callers.
type State struct {
	m map[string]any
}

// NewState returns an empty [State]. It is equivalent to State{} but reads more
// clearly at call sites such as Define(workflow.NewState(), ...).
func NewState() State {
	return State{}
}

// ToBuilder returns a [Builder] pre-populated with this state's values. Use it
// to derive a new State:
//
//	b := s.ToBuilder()
//	workflow.SetValue(b, Count, n+1)
//	next := b.Build()
func (s State) ToBuilder() *Builder {
	return &Builder{m: cloneMap(s.m)}
}

// Values returns a copy of all values held by this state. Mutating the returned
// map does not affect the state.
func (s State) Values() map[string]any {
	return cloneMap(s.m)
}

// IsTerminated reports whether this state was produced by [State.Terminate].
// The [ExecutionChain] engine skips all subsequent tasks for a terminated
// state and passes it straight through to the output.
func (s State) IsTerminated() bool {
	v, ok := s.m[terminatedKey].(bool)
	return ok && v
}

// Terminate returns a copy of this state flagged as terminated. A task returns
// state.Terminate() to deliberately and successfully short-circuit the chain
// (for example, "request is unauthorized, stop here").
func (s State) Terminate() State {
	b := s.ToBuilder()
	b.m[terminatedKey] = true
	return b.Build()
}

// String implements fmt.Stringer.
func (s State) String() string {
	return fmt.Sprintf("%v", s.m)
}

// MergeStates merges several states into one. Later states win on key
// conflicts. It is the canonical [ZipperFunc] for fan-in after a parallel
// level: ExecutionChain.NextWith(workflow.MergeStates, taskA, taskB).
func MergeStates(states ...State) State {
	b := NewBuilder()
	for _, s := range states {
		b.AddAll(s.m)
	}
	return b.Build()
}

// GetValue returns the value stored under key and whether it was present. When
// the key is absent it returns the zero value of T and false.
func GetValue[T any](s State, key StateKey[T]) (T, bool) {
	var zero T
	v, ok := s.m[key.Name]
	if !ok {
		return zero, false
	}
	typed, ok := v.(T)
	if !ok {
		// Stored under the same name with an incompatible type. Treat as
		// absent rather than panicking, mirroring a defensive read.
		return zero, false
	}
	return typed, true
}

// GetRequiredValue returns the value stored under key, or an error if it is missing.
func GetRequiredValue[T any](s State, key StateKey[T]) (T, error) {
	v, ok := GetValue(s, key)
	if !ok {
		var zero T
		return zero, fmt.Errorf("workflow: required state key %q is missing", key.Name)
	}
	return v, nil
}

// Builder accumulates values for a new [State]. It is mutable and not safe for
// concurrent use; build a State and share that instead.
type Builder struct {
	m map[string]any
}

// NewBuilder returns an empty [Builder].
func NewBuilder() *Builder {
	return &Builder{m: make(map[string]any)}
}

// SetValue stores value under key and returns the builder for chaining. It is a
// free function rather than a method because Go methods cannot introduce their
// own type parameter:
//
//	b := workflow.NewBuilder()
//	workflow.SetValue(b, Count, 1)
//	workflow.SetValue(b, Name, "alice")
func SetValue[T any](b *Builder, key StateKey[T], value T) *Builder {
	if b.m == nil {
		b.m = make(map[string]any)
	}
	b.m[key.Name] = value
	return b
}

// AddAll copies every entry from values into the builder and returns it for
// chaining. Existing keys are overwritten.
func (b *Builder) AddAll(values map[string]any) *Builder {
	if len(values) == 0 {
		return b
	}
	if b.m == nil {
		b.m = make(map[string]any, len(values))
	}
	for k, v := range values {
		b.m[k] = v
	}
	return b
}

// Build returns an immutable [State] holding the builder's current values. The
// builder may continue to be used afterwards; the returned State is unaffected
// by later mutations.
func (b *Builder) Build() State {
	return State{m: cloneMap(b.m)}
}

// cloneMap returns a shallow copy of m, or nil when m is empty. Values are not
// deep-copied; tasks are expected to treat stored values as immutable.
func cloneMap(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
