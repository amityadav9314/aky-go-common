package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"aky-go-common/logger"
)

// ZipperFunc merges the result states of a parallel level into a single state.
// It is invoked once per combination of upstream emissions (the cartesian
// product when several tasks at the level are multi-valued). [MergeStates] is
// the common implementation.
type ZipperFunc func(states ...State) State

// LogIDSupplier derives a correlation id from the initial state. The id is
// attached to time-logging and non-fatal-error log lines.
type LogIDSupplier func(State) string

// Field is a structured logging field. It is defined here (rather than
// re-exported from the project logger) so callers implementing [Logger] need
// not import zap or the logger's concrete field types.
type Field struct {
	Key   string
	Value any
}

// errorFieldKey is the reserved [Field] key used by [Err]. The default logger
// recognizes it and routes the value to the project logger's error parameter.
const errorFieldKey = "error"

// String builds a string-valued [Field].
func String(key, value string) Field { return Field{Key: key, Value: value} }

// Duration builds a duration-valued [Field].
func Duration(key string, d time.Duration) Field { return Field{Key: key, Value: d} }

// Err builds a [Field] carrying an error under the reserved error key.
func Err(err error) Field { return Field{Key: errorFieldKey, Value: err} }

// Logger is the minimal logging surface the engine needs. The default
// implementation forwards to the project's pkg/logger; inject your own with
// [ExecutionChain.WithLogger] to redirect or silence output.
type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// workflowServiceName labels engine log lines emitted to the project logger.
const workflowServiceName = "workflow"

// defaultLogger forwards engine logs to the project's background logger.
type defaultLogger struct{}

func (defaultLogger) Info(msg string, fields ...Field) {
	logger.GetBackgroundLogger().InfoMsg(
		context.Background(), msg, workflowServiceName,
		[]logger.MetricType{logger.LogFile}, toLoggingFields(fields)...,
	)
}

func (defaultLogger) Error(msg string, fields ...Field) {
	rest, err := splitErrorField(fields)
	logger.GetBackgroundLogger().ErrorMsg(
		context.Background(), msg, workflowServiceName, err,
		[]logger.MetricType{logger.LogFile}, rest...,
	)
}

// loggerFieldKey maps the engine's fixed field keys onto the logger's closed
// field vocabulary. The engine only ever emits the keys handled here.
func loggerFieldKey(key string) logger.FieldKey {
	switch key {
	case "task":
		return logger.FieldKeyTask
	case "log_id":
		return logger.FieldKeyLogID
	case "elapsed":
		return logger.FieldKeyElapsed
	default:
		return logger.FieldKeyExtraInfo
	}
}

// toLoggingFields converts engine fields into the project logger's field type.
func toLoggingFields(fields []Field) []logger.LoggingField {
	out := make([]logger.LoggingField, 0, len(fields))
	for _, f := range fields {
		out = append(out, logger.FieldAny(loggerFieldKey(f.Key), f.Value))
	}
	return out
}

// splitErrorField extracts the error carried by [Err] so it can be passed to
// the project logger's dedicated error parameter, returning the remaining
// fields already converted for the logger.
func splitErrorField(fields []Field) ([]logger.LoggingField, error) {
	var err error
	rest := make([]logger.LoggingField, 0, len(fields))
	for _, f := range fields {
		if f.Key == errorFieldKey {
			if e, ok := f.Value.(error); ok {
				err = e
				continue
			}
		}
		rest = append(rest, logger.FieldAny(loggerFieldKey(f.Key), f.Value))
	}
	return rest, err
}

var errNoTasks = errors.New("workflow: a level requires at least one task")

// ExecutionChain is a declarative, lazily-evaluated task pipeline. It is the Go
// counterpart of the Java RxJava / Python asyncio "ExecutionChain".
//
// # Building and running
//
// Start a chain with [Define] (or [DefineWith] to fan in with a zipper), append
// levels with [ExecutionChain.Next] / [ExecutionChain.NextWith], then run it
// with a terminal method:
//
//   - [ExecutionChain.Execute] — run and return the single result state.
//   - [ExecutionChain.Collect] — run and return every emitted state.
//   - [ExecutionChain.First]   — run and return the first emitted state.
//   - [ExecutionChain.Stream]  — the raw stream, for incremental consumption.
//   - [GetResult]              — Execute plus a typed read of one key.
//
// Nothing runs until a terminal method is called, the chain may be executed
// more than once, and a fully built chain is safe to execute concurrently
// (configuration via the With* methods is not).
//
// A level with one task runs sequentially per upstream state. A level with
// several tasks runs them in parallel; without a zipper their emissions are
// merged (fastest-first by default, or grouped by task order via
// [ExecutionChain.WithOrderedMerge]), and with a zipper they are fanned back
// into one state per combination.
//
// # Example: simple linear pipeline
//
//	var count = workflow.NewKey[int]("count")
//
//	type IncrementTask struct{}
//
//	func (IncrementTask) IsErrorFatal() bool { return true }
//
//	func (IncrementTask) Run(ctx context.Context, s workflow.State) (workflow.State, error) {
//		n, _ := workflow.GetValue(s, count) // absent -> zero value
//		b := s.ToBuilder()
//		workflow.SetValue(b, count, n+1)
//		return b.Build(), nil
//	}
//
//	chain := workflow.Define(workflow.NewState(), IncrementTask{}).Next(IncrementTask{})
//	final, err := chain.Execute(ctx)         // final[count] == 2
//
// # Example: termination (short-circuit)
//
// A task returns s.Terminate()'s result to deliberately and successfully stop
// the chain; all later tasks are skipped and the terminated state passes
// straight through to the output.
//
//	func (AuthTask) Run(ctx context.Context, s workflow.State) (workflow.State, error) {
//		if !authorized(s) {
//			b := s.ToBuilder()
//			workflow.SetValue(b, errKey, "Forbidden")
//			return b.Build().Terminate(), nil
//		}
//		return s, nil
//	}
//
//	chain := workflow.Define(workflow.NewState(), AuthTask{}).Next(ProcessDataTask{})
//	// ProcessDataTask does NOT run when AuthTask terminates.
//
// # Example: fan-out with a MultiValuedTask
//
// A [MultiValuedTask] emits many states; each becomes its own execution path
// for the remaining tasks.
//
//	type SplitWords struct{}
//
//	func (SplitWords) IsErrorFatal() bool { return true }
//
//	func (SplitWords) Stream(ctx context.Context, s workflow.State) iter.Seq2[workflow.State, error] {
//		return func(yield func(workflow.State, error) bool) {
//			text, err := workflow.GetRequiredValue(s, textKey)
//			if err != nil {
//				yield(workflow.State{}, err)
//				return
//			}
//			for _, w := range strings.Fields(text) {
//				b := s.ToBuilder()
//				workflow.SetValue(b, wordKey, w)
//				if !yield(b.Build(), nil) { // honor early consumer exit
//					return
//				}
//			}
//		}
//	}
//
//	chain := workflow.Define(initial, SplitWords{}).Next(UppercaseTask{})
//	for state, err := range chain.Stream(ctx) { // MUST break on error
//		if err != nil { break }
//		// ... use state ...
//	}
//	// or, more simply:
//	states, err := chain.Collect(ctx)
//
// # Example: parallel execution with a zipper (fan-in)
//
//	chain := workflow.Define(workflow.NewState(), PrepareTask{}).
//		NextWith(workflow.MergeStates, FetchUserTask{}, FetchOrdersTask{})
//	final, err := chain.Execute(ctx) // user and orders merged into one state
//
// When several tasks at a zipper level are multi-valued, the zipper is invoked
// once per combination of their emissions (the cartesian product).
//
// # Error handling
//
// Each task reports [Task.IsErrorFatal]. When a task returns an error:
//
//   - fatal: the error aborts the whole chain,
//     wrapped as "workflow: task <T>: <cause>".
//   - non-fatal: the error is logged and the previous, unmodified state flows
//     through as if the task never ran.
//
// This is distinct from termination: termination is a successful, deliberate
// stop; a fatal error is a failure. Across a parallel level the first failure
// cancels its siblings (via context) and concurrent failures are aggregated
// with errors.Join.
//
// # Cancellation, concurrency and observability
//
// The [context.Context] passed to a terminal method flows into every task, so
// caller deadlines and cancellation propagate to in-flight work. Bound parallel
// fan-out with [ExecutionChain.WithConcurrency], attach a correlation id with
// [ExecutionChain.WithLogID], enable per-task timing with
// [ExecutionChain.WithTimeLogging], and redirect or silence logging with
// [ExecutionChain.WithLogger]. Per-task timeouts and retries are available as
// composable middleware: [WithTimeout] and [WithRetry].
type ExecutionChain struct {
	initial State
	stream  stage

	logID       string
	timeLogging bool
	concurrency int // 0 means unbounded
	ordered     bool
	logger      Logger
}

// Define starts a chain from an initial state with one or more tasks at the
// first level. Multiple tasks run in parallel and their emissions are merged
// (use [DefineWith] to fan them back in with a zipper).
func Define(initial State, tasks ...Task) *ExecutionChain {
	return newChain(initial, nil, tasks)
}

// DefineWith is like [Define] but fans the first parallel level back into a
// single state per combination using zipper.
func DefineWith(initial State, zipper ZipperFunc, tasks ...Task) *ExecutionChain {
	return newChain(initial, zipper, tasks)
}

func newChain(initial State, zipper ZipperFunc, tasks []Task) *ExecutionChain {
	c := &ExecutionChain{
		initial: initial,
		logger:  defaultLogger{},
	}
	if len(tasks) == 0 {
		c.stream = func(context.Context) Stream { return errStream(errNoTasks) }
		return c
	}
	if len(tasks) > 1 {
		c.stream = func(ctx context.Context) Stream {
			return c.defineParallel(ctx, initial, zipper, tasks)
		}
	} else {
		task := tasks[0]
		c.stream = func(ctx context.Context) Stream {
			return c.runTask(ctx, initial, task)
		}
	}
	return c
}

// WithLogID sets a correlation-id supplier, evaluated immediately against the
// initial state. Returns the chain for fluent configuration.
func (c *ExecutionChain) WithLogID(f LogIDSupplier) *ExecutionChain {
	if f != nil {
		c.logID = f(c.initial)
	}
	return c
}

// WithTimeLogging enables per-task elapsed-time logging via the chain's logger.
func (c *ExecutionChain) WithTimeLogging(on bool) *ExecutionChain {
	c.timeLogging = on
	return c
}

// WithConcurrency bounds how many tasks at a parallel level run at once. A
// value <= 0 (the default) means unbounded.
func (c *ExecutionChain) WithConcurrency(n int) *ExecutionChain {
	c.concurrency = n
	return c
}

// WithOrderedMerge makes a zipper-less parallel level emit results grouped by
// task order instead of the default fastest-first interleaving. It has no
// effect on levels that use a zipper.
func (c *ExecutionChain) WithOrderedMerge(on bool) *ExecutionChain {
	c.ordered = on
	return c
}

// WithLogger overrides the chain's logger. A nil logger is ignored.
func (c *ExecutionChain) WithLogger(l Logger) *ExecutionChain {
	if l != nil {
		c.logger = l
	}
	return c
}

// Next appends a level. With one task the level runs sequentially per upstream
// state; with several it runs them in parallel and merges their emissions.
func (c *ExecutionChain) Next(tasks ...Task) *ExecutionChain {
	return c.appendLevel(nil, tasks)
}

// NextWith appends a parallel level whose results are fanned back into a single
// state per combination using zipper.
func (c *ExecutionChain) NextWith(zipper ZipperFunc, tasks ...Task) *ExecutionChain {
	return c.appendLevel(zipper, tasks)
}

func (c *ExecutionChain) appendLevel(zipper ZipperFunc, tasks []Task) *ExecutionChain {
	previous := c.stream
	switch {
	case len(tasks) == 0:
		c.stream = func(context.Context) Stream { return errStream(errNoTasks) }
	case len(tasks) > 1:
		c.stream = func(ctx context.Context) Stream {
			return func(yield func(State, error) bool) {
				for state, err := range previous(ctx) {
					if err != nil {
						emitErr(yield, err)
						return
					}
					if state.IsTerminated() {
						if !yield(state, nil) {
							return
						}
						continue
					}
					for ns, nerr := range c.defineParallel(ctx, state, zipper, tasks) {
						if !yield(ns, nerr) {
							return
						}
						if nerr != nil {
							return
						}
					}
				}
			}
		}
	default:
		c.stream = c.chainSingle(previous, tasks[0])
	}
	return c
}

// Stream returns the raw result stream. Callers MUST stop iterating as soon as
// the error value is non-nil (the standard range-over-func error contract).
// Prefer [ExecutionChain.Execute] / [ExecutionChain.Collect] /
// [ExecutionChain.First] unless you need to process emissions incrementally.
func (c *ExecutionChain) Stream(ctx context.Context) Stream {
	return c.stream(ctx)
}

// Execute runs the chain and returns its single result state. It errors if the
// chain fails or emits other than exactly one state (e.g. it fanned out).
func (c *ExecutionChain) Execute(ctx context.Context) (State, error) {
	return single(c.stream(ctx))
}

// Collect runs the chain and returns every emitted state, stopping at the first
// error.
func (c *ExecutionChain) Collect(ctx context.Context) ([]State, error) {
	return collect(c.stream(ctx))
}

// First runs the chain and returns its first emitted state.
func (c *ExecutionChain) First(ctx context.Context) (State, error) {
	return first(c.stream(ctx))
}

// GetResult executes the chain (expecting a single result) and reads a typed
// value from the final state. It returns the zero value of T when the key is
// absent.
func GetResult[T any](ctx context.Context, c *ExecutionChain, key StateKey[T]) (T, error) {
	state, err := c.Execute(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	v, _ := GetValue(state, key)
	return v, nil
}

// ---------------------------------------------------------------------------
// Internal engine
// ---------------------------------------------------------------------------

// adapt unifies the two task flavors into a single stream-producing function so
// the rest of the engine never type-switches in its hot path.
func adapt(task Task) func(ctx context.Context, prev State) Stream {
	switch t := task.(type) {
	case SimpleTask:
		return func(ctx context.Context, prev State) Stream {
			return func(yield func(State, error) bool) {
				s, err := t.Run(ctx, prev)
				if err != nil {
					emitErr(yield, err)
					return
				}
				yield(s, nil)
			}
		}
	case MultiValuedTask:
		return t.Stream
	default:
		return func(context.Context, State) Stream {
			return errStream(fmt.Errorf(
				"workflow: task %s implements neither SimpleTask nor MultiValuedTask", taskName(task)))
		}
	}
}

// chainSingle runs task once per upstream state, passing terminated states
// straight through.
func (c *ExecutionChain) chainSingle(previous stage, task Task) stage {
	return func(ctx context.Context) Stream {
		return func(yield func(State, error) bool) {
			for state, err := range previous(ctx) {
				if err != nil {
					emitErr(yield, err)
					return
				}
				if state.IsTerminated() {
					if !yield(state, nil) {
						return
					}
					continue
				}
				for ns, nerr := range c.runTask(ctx, state, task) {
					if !yield(ns, nerr) {
						return
					}
					if nerr != nil {
						return
					}
				}
			}
		}
	}
}

// runTask executes a single task against prev, applying time logging and the
// fatal/non-fatal error policy. On a non-fatal error it logs and resumes with
// the unmodified previous state, exactly like the Java/Python originals.
func (c *ExecutionChain) runTask(ctx context.Context, prev State, task Task) Stream {
	return func(yield func(State, error) bool) {
		start := time.Now()
		if c.timeLogging {
			defer func() {
				c.logger.Info("workflow task completed",
					String("task", taskName(task)),
					String("log_id", c.logID),
					Duration("elapsed", time.Since(start)),
				)
			}()
		}

		for s, err := range adapt(task)(ctx, prev) {
			if err != nil {
				if task.IsErrorFatal() {
					emitErr(yield, fmt.Errorf("workflow: task %s: %w", taskName(task), err))
					return
				}
				c.logger.Error("workflow non-fatal task error",
					String("task", taskName(task)),
					String("log_id", c.logID),
					Err(err),
				)
				yield(prev, nil) // resume with previous state, then stop this task
				return
			}
			if !yield(s, nil) {
				return
			}
		}
	}
}

// defineParallel dispatches a multi-task level to the right strategy.
func (c *ExecutionChain) defineParallel(ctx context.Context, state State, zipper ZipperFunc, tasks []Task) Stream {
	if zipper == nil {
		if c.ordered {
			return c.runMergeOrdered(ctx, state, tasks)
		}
		return c.runMergeUnordered(ctx, state, tasks)
	}
	return c.runZip(ctx, state, zipper, tasks)
}

// runMergeUnordered runs all tasks concurrently and yields their emissions as
// they arrive (fastest-first). First failure cancels the siblings; concurrent
// failures are aggregated with errors.Join.
func (c *ExecutionChain) runMergeUnordered(ctx context.Context, state State, tasks []Task) Stream {
	return func(yield func(State, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		ch := make(chan State)
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
			sem  chan struct{}
		)
		if c.concurrency > 0 {
			sem = make(chan struct{}, c.concurrency)
		}

		for _, task := range tasks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if sem != nil {
					select {
					case sem <- struct{}{}:
						defer func() { <-sem }()
					case <-ctx.Done():
						return
					}
				}
				for s, err := range c.runTask(ctx, state, task) {
					if err != nil {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
						cancel()
						return
					}
					select {
					case ch <- s:
					case <-ctx.Done():
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(ch)
		}()

		for s := range ch {
			if !yield(s, nil) {
				cancel()
				for range ch { //nolint:revive // drain so producers can exit
				}
				return
			}
		}

		mu.Lock()
		err := joinTaskErrors(errs)
		mu.Unlock()
		if err != nil {
			emitErr(yield, err)
		}
	}
}

// runMergeOrdered runs all tasks concurrently but emits their results grouped
// by task order.
func (c *ExecutionChain) runMergeOrdered(ctx context.Context, state State, tasks []Task) Stream {
	return func(yield func(State, error) bool) {
		results, err := c.collectParallel(ctx, state, tasks)
		if err != nil {
			emitErr(yield, err)
			return
		}
		for _, states := range results {
			for _, s := range states {
				if !yield(s, nil) {
					return
				}
			}
		}
	}
}

// runZip runs all tasks concurrently, then yields zipper(combo...) for every
// combination of their emissions (the cartesian product).
func (c *ExecutionChain) runZip(ctx context.Context, state State, zipper ZipperFunc, tasks []Task) Stream {
	return func(yield func(State, error) bool) {
		results, err := c.collectParallel(ctx, state, tasks)
		if err != nil {
			emitErr(yield, err)
			return
		}
		cartesianEach(results, func(combo []State) bool {
			return yield(zipper(combo...), nil)
		})
	}
}

// collectParallel runs each task to completion concurrently (bounded by
// WithConcurrency) and returns their emissions indexed by task position. The
// first failure cancels the rest; all failures are aggregated.
func (c *ExecutionChain) collectParallel(ctx context.Context, state State, tasks []Task) ([][]State, error) {
	g, gctx := errgroup.WithContext(ctx)
	if c.concurrency > 0 {
		g.SetLimit(c.concurrency)
	}
	results := make([][]State, len(tasks))
	var (
		mu   sync.Mutex
		errs []error
	)
	for i, task := range tasks {
		g.Go(func() error {
			states, err := collect(c.runTask(gctx, state, task))
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return err // cancel siblings via errgroup's context
			}
			results[i] = states
			return nil
		})
	}
	_ = g.Wait()
	if err := joinTaskErrors(errs); err != nil {
		return nil, err
	}
	return results, nil
}

// joinTaskErrors aggregates task errors, preferring genuine failures over the
// context-cancellation noise produced when the first failure aborts siblings.
func joinTaskErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	var real []error
	for _, e := range errs {
		if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			real = append(real, e)
		}
	}
	if len(real) > 0 {
		return errors.Join(real...)
	}
	return errors.Join(errs...)
}

// cartesianEach calls fn for each combination across sets, generating
// combinations lazily (odometer style) so the full product is never
// materialized. Iteration stops early if fn returns false. An empty set makes
// the product empty.
func cartesianEach(sets [][]State, fn func([]State) bool) {
	if len(sets) == 0 {
		return
	}
	for _, s := range sets {
		if len(s) == 0 {
			return
		}
	}
	idx := make([]int, len(sets))
	for {
		combo := make([]State, len(sets))
		for i := range sets {
			combo[i] = sets[i][idx[i]]
		}
		if !fn(combo) {
			return
		}
		pos := len(sets) - 1
		for pos >= 0 {
			idx[pos]++
			if idx[pos] < len(sets[pos]) {
				break
			}
			idx[pos] = 0
			pos--
		}
		if pos < 0 {
			return
		}
	}
}

// errStream returns a stream that yields a single fatal error.
func errStream(err error) Stream {
	return func(yield func(State, error) bool) {
		yield(State{}, err)
	}
}
