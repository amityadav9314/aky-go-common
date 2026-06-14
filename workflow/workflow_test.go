package workflow

import (
	"context"
	"errors"
	"iter"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amityadav9314/aky-go-common/logger"
)

// nopLogger silences engine log output during tests.
type nopLogger struct{}

func (nopLogger) Info(string, ...Field)  {}
func (nopLogger) Error(string, ...Field) {}

type loggedCall struct {
	msg    string
	fields []Field
}

type captureLogger struct {
	infos  []loggedCall
	errors []loggedCall
}

func (c *captureLogger) Info(msg string, fields ...Field) {
	c.infos = append(c.infos, loggedCall{msg: msg, fields: append([]Field(nil), fields...)})
}

func (c *captureLogger) Error(msg string, fields ...Field) {
	c.errors = append(c.errors, loggedCall{msg: msg, fields: append([]Field(nil), fields...)})
}

// ---------------------------------------------------------------------------
// Shared test keys and tasks
// ---------------------------------------------------------------------------

var (
	countKey  = NewKey[int]("count")
	wordKey   = NewKey[string]("word")
	textKey   = NewKey[string]("text")
	userKey   = NewKey[string]("user")
	ordersKey = NewKey[[]string]("orders")
	errKey    = NewKey[string]("error")
	letterKey = NewKey[string]("letter")
	digitKey  = NewKey[int]("digit")
)

type incrementTask struct{}

func (incrementTask) IsErrorFatal() bool { return true }

func (incrementTask) Run(_ context.Context, s State) (State, error) {
	n, _ := GetValue(s, countKey)
	b := s.ToBuilder()
	SetValue(b, countKey, n+1)
	return b.Build(), nil
}

// splitWordsTask is a MultiValuedTask: it fans out one state per word.
type splitWordsTask struct{}

func (splitWordsTask) IsErrorFatal() bool { return true }

func (splitWordsTask) Stream(_ context.Context, s State) iter.Seq2[State, error] {
	return func(yield func(State, error) bool) {
		text, err := GetRequiredValue(s, textKey)
		if err != nil {
			yield(State{}, err)
			return
		}
		for _, w := range strings.Fields(text) {
			b := s.ToBuilder()
			SetValue(b, wordKey, w)
			if !yield(b.Build(), nil) {
				return
			}
		}
	}
}

type uppercaseTask struct{}

func (uppercaseTask) IsErrorFatal() bool { return true }

func (uppercaseTask) Run(_ context.Context, s State) (State, error) {
	w, err := GetRequiredValue(s, wordKey)
	if err != nil {
		return State{}, err
	}
	b := s.ToBuilder()
	SetValue(b, wordKey, strings.ToUpper(w))
	return b.Build(), nil
}

// ---------------------------------------------------------------------------
// 1. Simple linear pipeline
// ---------------------------------------------------------------------------

func TestLinearPipeline(t *testing.T) {
	chain := Define(NewState(), incrementTask{}).Next(incrementTask{})

	final, err := chain.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got, _ := GetValue(final, countKey); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
}

func TestGetResult(t *testing.T) {
	chain := Define(NewState(), incrementTask{}).Next(incrementTask{}).Next(incrementTask{})

	got, err := GetResult(context.Background(), chain, countKey)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestChainIsReusable(t *testing.T) {
	chain := Define(NewState(), incrementTask{}).Next(incrementTask{})
	for i := 0; i < 3; i++ {
		final, err := chain.Execute(context.Background())
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if got, _ := GetValue(final, countKey); got != 2 {
			t.Fatalf("run %d: count = %d, want 2", i, got)
		}
	}
}

func TestNoTasksErrors(t *testing.T) {
	_, err := Define(NewState()).Execute(context.Background())
	if !errors.Is(err, errNoTasks) {
		t.Fatalf("Define without tasks error = %v, want errNoTasks", err)
	}

	_, err = Define(NewState(), incrementTask{}).Next().Execute(context.Background())
	if !errors.Is(err, errNoTasks) {
		t.Fatalf("Next without tasks error = %v, want errNoTasks", err)
	}
}

func TestFirst(t *testing.T) {
	firstState, err := Define(NewState(), incrementTask{}).First(context.Background())
	if err != nil {
		t.Fatalf("First: %v", err)
	}
	if got, _ := GetValue(firstState, countKey); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}

	_, err = Define(NewState()).First(context.Background())
	if !errors.Is(err, errNoTasks) {
		t.Fatalf("First error = %v, want errNoTasks", err)
	}

	_, err = Define(NewState(), emptyMultiTask{}).First(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stream emitted no values") {
		t.Fatalf("First empty error = %v, want empty stream error", err)
	}
}

func TestExecuteRequiresSingleResult(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, textKey, "a b")

	_, err := Define(ib.Build(), splitWordsTask{}).Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "expected exactly one result, got 2") {
		t.Fatalf("Execute multi-result error = %v", err)
	}

	_, err = Define(NewState(), emptyMultiTask{}).Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "expected exactly one result, got 0") {
		t.Fatalf("Execute empty error = %v", err)
	}
}

func TestGetResultReturnsZeroOnExecutionError(t *testing.T) {
	got, err := GetResult(context.Background(), Define(NewState(), failingTask{fatal: true}), countKey)
	if err == nil {
		t.Fatal("expected GetResult error")
	}
	if got != 0 {
		t.Fatalf("GetResult returned %d, want zero value", got)
	}
}

// ---------------------------------------------------------------------------
// 2. Termination (short-circuit)
// ---------------------------------------------------------------------------

type authTask struct {
	authorized bool
}

func (authTask) IsErrorFatal() bool { return true }

func (a authTask) Run(_ context.Context, s State) (State, error) {
	if !a.authorized {
		b := s.ToBuilder()
		SetValue(b, errKey, "Forbidden")
		return b.Build().Terminate(), nil
	}
	return s, nil
}

type mustNotRunTask struct {
	ran *atomic.Bool
}

func (mustNotRunTask) IsErrorFatal() bool { return true }

func (m mustNotRunTask) Run(_ context.Context, s State) (State, error) {
	m.ran.Store(true)
	return s, nil
}

func TestTerminationSkipsLaterTasks(t *testing.T) {
	var ran atomic.Bool
	chain := Define(NewState(), authTask{authorized: false}).
		Next(mustNotRunTask{ran: &ran})

	final, err := chain.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !final.IsTerminated() {
		t.Fatal("expected terminated state")
	}
	if msg, _ := GetValue(final, errKey); msg != "Forbidden" {
		t.Fatalf("error = %q, want Forbidden", msg)
	}
	if ran.Load() {
		t.Fatal("downstream task ran despite termination")
	}
}

func TestNoTerminationRunsLaterTasks(t *testing.T) {
	var ran atomic.Bool
	chain := Define(NewState(), authTask{authorized: true}).
		Next(mustNotRunTask{ran: &ran})

	if _, err := chain.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !ran.Load() {
		t.Fatal("downstream task should have run")
	}
}

func TestTerminationSkipsParallelLevel(t *testing.T) {
	var ran atomic.Bool
	chain := Define(NewState(), authTask{authorized: false}).
		Next(mustNotRunTask{ran: &ran}, mustNotRunTask{ran: &ran})

	states, err := chain.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(states) != 1 || !states[0].IsTerminated() {
		t.Fatalf("states = %v, want one terminated state", states)
	}
	if ran.Load() {
		t.Fatal("parallel level ran despite termination")
	}
}

func TestTerminationParallelLevelConsumerStops(t *testing.T) {
	var ran atomic.Bool
	for range Define(NewState(), authTask{authorized: false}).
		Next(mustNotRunTask{ran: &ran}, mustNotRunTask{ran: &ran}).
		Stream(context.Background()) {
		break
	}
	if ran.Load() {
		t.Fatal("parallel level ran despite termination")
	}
}

// ---------------------------------------------------------------------------
// 3. Fan-out with a MultiValuedTask
// ---------------------------------------------------------------------------

func TestFanOut(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, textKey, "hello world")

	chain := Define(ib.Build(), splitWordsTask{}).Next(uppercaseTask{})

	states, err := chain.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var words []string
	for _, s := range states {
		w, _ := GetValue(s, wordKey)
		words = append(words, w)
	}
	sort.Strings(words)
	if strings.Join(words, ",") != "HELLO,WORLD" {
		t.Fatalf("words = %v, want [HELLO WORLD]", words)
	}
}

func TestStreamRaw(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, textKey, "a b c")

	chain := Define(ib.Build(), splitWordsTask{})

	count := 0
	for s, err := range chain.Stream(context.Background()) {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		if _, ok := GetValue(s, wordKey); !ok {
			t.Fatal("missing word in emitted state")
		}
		count++
	}
	if count != 3 {
		t.Fatalf("emitted %d states, want 3", count)
	}
}

// ---------------------------------------------------------------------------
// 4. Parallel execution with a zipper (fan-in)
// ---------------------------------------------------------------------------

type fetchUserTask struct{}

func (fetchUserTask) IsErrorFatal() bool { return true }

func (fetchUserTask) Run(_ context.Context, s State) (State, error) {
	time.Sleep(5 * time.Millisecond)
	b := s.ToBuilder()
	SetValue(b, userKey, "Alice")
	return b.Build(), nil
}

type fetchOrdersTask struct{}

func (fetchOrdersTask) IsErrorFatal() bool { return true }

func (fetchOrdersTask) Run(_ context.Context, s State) (State, error) {
	b := s.ToBuilder()
	SetValue(b, ordersKey, []string{"A", "B"})
	return b.Build(), nil
}

func TestParallelZipper(t *testing.T) {
	chain := Define(NewState(), incrementTask{}).
		NextWith(MergeStates, fetchUserTask{}, fetchOrdersTask{})

	final, err := chain.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if u, _ := GetValue(final, userKey); u != "Alice" {
		t.Fatalf("user = %q, want Alice", u)
	}
	if o, _ := GetValue(final, ordersKey); strings.Join(o, "") != "AB" {
		t.Fatalf("orders = %v, want [A B]", o)
	}
	if n, _ := GetValue(final, countKey); n != 1 {
		t.Fatalf("count = %d, want 1 (preserved from upstream)", n)
	}
}

func TestParallelMergeNoZipper(t *testing.T) {
	// Without a zipper, a parallel level emits one state per task.
	chain := Define(NewState(), fetchUserTask{}, fetchOrdersTask{})

	states, err := chain.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("emitted %d states, want 2", len(states))
	}
}

func TestOrderedMerge(t *testing.T) {
	// fetchUser sleeps, so fastest-first would put orders before user; ordered
	// merge must keep task order (user first).
	chain := Define(NewState(), fetchUserTask{}, fetchOrdersTask{}).WithOrderedMerge(true)

	states, err := chain.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("emitted %d states, want 2", len(states))
	}
	if _, ok := GetValue(states[0], userKey); !ok {
		t.Fatal("ordered merge: first state should come from fetchUserTask")
	}
	if _, ok := GetValue(states[1], ordersKey); !ok {
		t.Fatal("ordered merge: second state should come from fetchOrdersTask")
	}
}

// ---------------------------------------------------------------------------
// 5. Non-fatal error resumes with the previous state
// ---------------------------------------------------------------------------

type failingTask struct {
	fatal bool
}

func (f failingTask) IsErrorFatal() bool { return f.fatal }
func (failingTask) Run(_ context.Context, _ State) (State, error) {
	return State{}, errors.New("boom")
}

func TestNonFatalErrorResumes(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, countKey, 7)

	chain := Define(ib.Build(), failingTask{fatal: false}).
		WithLogger(nopLogger{}).
		Next(incrementTask{})

	final, err := chain.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// failing task is swallowed (prev state flows through), then +1.
	if n, _ := GetValue(final, countKey); n != 8 {
		t.Fatalf("count = %d, want 8", n)
	}
}

func TestFatalErrorAborts(t *testing.T) {
	chain := Define(NewState(), failingTask{fatal: true}).Next(incrementTask{})

	_, err := chain.Execute(context.Background())
	if err == nil {
		t.Fatal("expected fatal error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error %q should wrap the underlying cause", err)
	}
}

// ---------------------------------------------------------------------------
// 6. Two multi-valued tasks under a zipper -> cartesian product
// ---------------------------------------------------------------------------

type lettersTask struct{}

func (lettersTask) IsErrorFatal() bool { return true }

func (lettersTask) Stream(_ context.Context, s State) iter.Seq2[State, error] {
	return func(yield func(State, error) bool) {
		for _, l := range []string{"a", "b"} {
			b := s.ToBuilder()
			SetValue(b, letterKey, l)
			if !yield(b.Build(), nil) {
				return
			}
		}
	}
}

type digitsTask struct{}

func (digitsTask) IsErrorFatal() bool { return true }

func (digitsTask) Stream(_ context.Context, s State) iter.Seq2[State, error] {
	return func(yield func(State, error) bool) {
		for _, d := range []int{1, 2, 3} {
			b := s.ToBuilder()
			SetValue(b, digitKey, d)
			if !yield(b.Build(), nil) {
				return
			}
		}
	}
}

func TestCartesianProduct(t *testing.T) {
	chain := DefineWith(NewState(), MergeStates, lettersTask{}, digitsTask{})

	states, err := chain.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(states) != 6 { // 2 letters x 3 digits
		t.Fatalf("got %d combinations, want 6", len(states))
	}
	seen := make(map[string]bool)
	for _, s := range states {
		l, _ := GetValue(s, letterKey)
		d, _ := GetValue(s, digitKey)
		seen[l+itoa(d)] = true
	}
	for _, want := range []string{"a1", "a2", "a3", "b1", "b2", "b3"} {
		if !seen[want] {
			t.Fatalf("missing combination %q (got %v)", want, seen)
		}
	}
}

type emptyMultiTask struct{}

func (emptyMultiTask) IsErrorFatal() bool { return true }

func (emptyMultiTask) Stream(context.Context, State) iter.Seq2[State, error] {
	return func(func(State, error) bool) {}
}

func TestCartesianProductWithEmptyInputEmitsNothing(t *testing.T) {
	states, err := DefineWith(NewState(), MergeStates, lettersTask{}, emptyMultiTask{}).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("got %d states, want 0", len(states))
	}
}

func TestCartesianEachEmptySets(t *testing.T) {
	called := false
	cartesianEach(nil, func([]State) bool {
		called = true
		return true
	})
	if called {
		t.Fatal("cartesianEach called callback for nil sets")
	}
}

// ---------------------------------------------------------------------------
// Go-native behavior: context cancellation, bounded concurrency, middleware
// ---------------------------------------------------------------------------

type blockingTask struct {
	started chan struct{}
	once    atomicOnce
}

func (*blockingTask) IsErrorFatal() bool { return true }

func (b *blockingTask) Run(ctx context.Context, s State) (State, error) {
	b.once.do(func() { close(b.started) })
	<-ctx.Done() // block until cancelled
	return State{}, ctx.Err()
}

func TestContextCancellation(t *testing.T) {
	bt := &blockingTask{started: make(chan struct{})}
	chain := Define(NewState(), bt)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-bt.started
		cancel()
	}()

	_, err := chain.Execute(ctx)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v should wrap context.Canceled", err)
	}
}

func TestBoundedConcurrency(t *testing.T) {
	var inFlight, maxSeen atomic.Int64
	mk := func() Task {
		return concurrencyProbe{inFlight: &inFlight, maxSeen: &maxSeen}
	}
	// The zipper path (collectParallel) honors errgroup's SetLimit.
	chain := DefineWith(NewState(), MergeStates, mk(), mk(), mk(), mk(), mk(), mk()).
		WithConcurrency(2)
	if _, err := chain.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if m := maxSeen.Load(); m > 2 {
		t.Fatalf("max concurrent = %d, want <= 2", m)
	}
}

func TestBoundedConcurrencyUnorderedMerge(t *testing.T) {
	var inFlight, maxSeen atomic.Int64
	mk := func() Task {
		return concurrencyProbe{inFlight: &inFlight, maxSeen: &maxSeen}
	}
	chain := Define(NewState(), mk(), mk(), mk(), mk(), mk(), mk()).WithConcurrency(2)
	if _, err := chain.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if m := maxSeen.Load(); m > 2 {
		t.Fatalf("max concurrent = %d, want <= 2", m)
	}
}

type concurrencyProbe struct {
	inFlight *atomic.Int64
	maxSeen  *atomic.Int64
}

func (concurrencyProbe) IsErrorFatal() bool { return true }

func (c concurrencyProbe) Run(_ context.Context, s State) (State, error) {
	n := c.inFlight.Add(1)
	for {
		m := c.maxSeen.Load()
		if n <= m || c.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	time.Sleep(10 * time.Millisecond)
	c.inFlight.Add(-1)
	return s, nil
}

func TestWithRetrySucceedsAfterFailures(t *testing.T) {
	var attempts atomic.Int64
	task := simpleFunc{
		fatal: true,
		name:  "flaky",
		run: func(_ context.Context, s State) (State, error) {
			if attempts.Add(1) < 3 {
				return State{}, errors.New("transient")
			}
			b := s.ToBuilder()
			SetValue(b, countKey, 99)
			return b.Build(), nil
		},
	}

	chain := Define(NewState(), WithRetry(3, task))
	final, err := chain.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if n, _ := GetValue(final, countKey); n != 99 {
		t.Fatalf("count = %d, want 99", n)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
}

func TestWithRetryFailureAndCancellation(t *testing.T) {
	var attempts atomic.Int64
	task := simpleFunc{
		fatal: true,
		name:  "alwaysFails",
		run: func(context.Context, State) (State, error) {
			attempts.Add(1)
			return State{}, errors.New("still broken")
		},
	}

	_, err := Define(NewState(), WithRetry(2, task)).Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed after 2 attempts") {
		t.Fatalf("retry error = %v, want attempts exhausted", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Define(NewState(), WithRetry(2, task)).Execute(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry cancellation error = %v, want context.Canceled", err)
	}
}

func TestWithRetryNoopCases(t *testing.T) {
	task := incrementTask{}
	if got := WithRetry(1, task); got != task {
		t.Fatal("WithRetry attempts <= 1 should return original simple task")
	}

	mt := lettersTask{}
	if got := WithRetry(3, mt); got != mt {
		t.Fatal("WithRetry multi-valued task should return original task")
	}
}

func TestWithTimeout(t *testing.T) {
	bt := &blockingTask{started: make(chan struct{})}
	chain := Define(NewState(), WithTimeout(20*time.Millisecond, bt))

	_, err := chain.Execute(context.Background())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v should wrap context.DeadlineExceeded", err)
	}
}

func TestWithTimeoutNoopAndMultiValued(t *testing.T) {
	task := incrementTask{}
	if got := WithTimeout(0, task); got != task {
		t.Fatal("WithTimeout non-positive duration should return original task")
	}

	wrapped := WithTimeout(time.Second, lettersTask{})
	mt, ok := wrapped.(MultiValuedTask)
	if !ok {
		t.Fatalf("wrapped task type = %T, want MultiValuedTask", wrapped)
	}
	states, err := collect(mt.Stream(context.Background(), NewState()))
	if err != nil {
		t.Fatalf("collect wrapped stream: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("wrapped stream emitted %d states, want 2", len(states))
	}

	invalid := invalidTask{}
	if got := WithTimeout(time.Second, invalid); got != invalid {
		t.Fatal("WithTimeout invalid task should return original task")
	}
}

func TestWithTimeoutMultiValuedStopsWhenConsumerStops(t *testing.T) {
	wrapped := WithTimeout(time.Second, lettersTask{}).(MultiValuedTask)
	count := 0
	for range wrapped.Stream(context.Background(), NewState()) {
		count++
		break
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

type contextAwareMultiTask struct{}

func (contextAwareMultiTask) IsErrorFatal() bool { return true }

func (contextAwareMultiTask) Stream(ctx context.Context, s State) iter.Seq2[State, error] {
	return func(yield func(State, error) bool) {
		<-ctx.Done()
		yield(State{}, ctx.Err())
	}
}

func TestWithTimeoutMultiValuedDeadline(t *testing.T) {
	_, err := Define(NewState(), WithTimeout(time.Millisecond, contextAwareMultiTask{})).
		Collect(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
}

type invalidTask struct{}

func (invalidTask) IsErrorFatal() bool { return true }

func TestInvalidTaskTypeErrors(t *testing.T) {
	_, err := Define(NewState(), invalidTask{}).Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "implements neither SimpleTask nor MultiValuedTask") {
		t.Fatalf("invalid task error = %v", err)
	}
}

func TestParallelErrorPaths(t *testing.T) {
	_, err := Define(NewState(), fetchUserTask{}, failingTask{fatal: true}).
		Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unordered merge error = %v, want boom", err)
	}

	_, err = Define(NewState(), fetchUserTask{}, failingTask{fatal: true}).
		WithOrderedMerge(true).
		Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("ordered merge error = %v, want boom", err)
	}

	_, err = DefineWith(NewState(), MergeStates, fetchUserTask{}, failingTask{fatal: true}).
		Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("zip error = %v, want boom", err)
	}
}

type cancelOnlyTask struct{}

func (cancelOnlyTask) IsErrorFatal() bool { return true }

func (cancelOnlyTask) Run(ctx context.Context, _ State) (State, error) {
	<-ctx.Done()
	return State{}, ctx.Err()
}

func TestParallelCancellationOnlyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Define(NewState(), cancelOnlyTask{}, cancelOnlyTask{}).Collect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unordered cancellation error = %v, want context.Canceled", err)
	}

	_, err = DefineWith(NewState(), MergeStates, cancelOnlyTask{}, cancelOnlyTask{}).Collect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("zip cancellation error = %v, want context.Canceled", err)
	}
}

func TestParallelSemaphoreStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _ = Define(NewState(), cancelOnlyTask{}, cancelOnlyTask{}).
		WithConcurrency(1).
		Collect(ctx)
}

func TestEarlyConsumerStops(t *testing.T) {
	for range Define(NewState(), lettersTask{}).Next(incrementTask{}).Stream(context.Background()) {
		break
	}

	for range Define(NewState(), fetchUserTask{}, fetchOrdersTask{}).Stream(context.Background()) {
		break
	}

	for range Define(NewState(), fetchUserTask{}, fetchOrdersTask{}).
		WithOrderedMerge(true).
		Stream(context.Background()) {
		break
	}

	for range DefineWith(NewState(), MergeStates, lettersTask{}, digitsTask{}).Stream(context.Background()) {
		break
	}

	for range Define(NewState(), lettersTask{}).
		Next(fetchUserTask{}, fetchOrdersTask{}).
		Stream(context.Background()) {
		break
	}
}

func TestSequentialTerminatedConsumerStops(t *testing.T) {
	for range Define(NewState(), authTask{authorized: false}).
		Next(incrementTask{}).
		Stream(context.Background()) {
		break
	}
}

func TestPreviousErrorBeforeParallelLevel(t *testing.T) {
	_, err := Define(NewState(), failingTask{fatal: true}).
		Next(fetchUserTask{}, fetchOrdersTask{}).
		Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestParallelLevelErrorContinuesUntilInternalStop(t *testing.T) {
	seq := Define(NewState(), incrementTask{}).
		Next(failingTask{fatal: true}, fetchOrdersTask{}).
		Stream(context.Background())

	seenErr := false
	seq(func(_ State, err error) bool {
		if err != nil {
			seenErr = true
		}
		return true
	})
	if !seenErr {
		t.Fatal("expected parallel level error")
	}
}

func TestSequentialTaskErrorContinuesUntilInternalStop(t *testing.T) {
	seq := Define(NewState(), incrementTask{}).
		Next(failingTask{fatal: true}).
		Stream(context.Background())

	seenErr := false
	seq(func(_ State, err error) bool {
		if err != nil {
			seenErr = true
		}
		return true
	})
	if !seenErr {
		t.Fatal("expected sequential task error")
	}
}

func TestConfigurationAndLoggerHelpers(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, textKey, "trace-123")
	log := &captureLogger{}
	chain := Define(ib.Build(), incrementTask{}).
		WithLogID(func(s State) string {
			v, _ := GetValue(s, textKey)
			return v
		}).
		WithLogID(nil).
		WithLogger(nil).
		WithLogger(log).
		WithTimeLogging(true)

	if _, err := chain.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(log.infos) != 1 {
		t.Fatalf("info logs = %d, want 1", len(log.infos))
	}
	if fieldValue(log.infos[0].fields, "log_id") != "trace-123" {
		t.Fatalf("log_id field = %v, want trace-123", fieldValue(log.infos[0].fields, "log_id"))
	}
	if _, ok := fieldValue(log.infos[0].fields, "elapsed").(time.Duration); !ok {
		t.Fatalf("elapsed field = %T, want time.Duration", fieldValue(log.infos[0].fields, "elapsed"))
	}

	d := time.Second
	if got := Duration("elapsed", d); got.Key != "elapsed" || got.Value != d {
		t.Fatalf("Duration field = %#v", got)
	}

	if got := loggerFieldKey("task"); got != logger.FieldKeyTask {
		t.Fatalf("task key = %v", got)
	}
	if got := loggerFieldKey("log_id"); got != logger.FieldKeyLogID {
		t.Fatalf("log id key = %v", got)
	}
	if got := loggerFieldKey("elapsed"); got != logger.FieldKeyElapsed {
		t.Fatalf("elapsed key = %v", got)
	}
	if got := loggerFieldKey("other"); got != logger.FieldKeyExtraInfo {
		t.Fatalf("other key = %v", got)
	}

	fields := []Field{String("task", "x"), Err(errors.New("boom")), Field{Key: errorFieldKey, Value: "not-error"}}
	if converted := toLoggingFields(fields[:1]); len(converted) != 1 {
		t.Fatalf("converted fields len = %d, want 1", len(converted))
	}
	rest, err := splitErrorField(fields)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("split error = %v, want boom", err)
	}
	if len(rest) != 2 {
		t.Fatalf("rest len = %d, want 2", len(rest))
	}

	defaultLogger{}.Info("workflow test info", String("task", "unit"))
	defaultLogger{}.Error("workflow test error", String("task", "unit"), Err(errors.New("boom")))
}

func fieldValue(fields []Field, key string) any {
	for _, f := range fields {
		if f.Key == key {
			return f.Value
		}
	}
	return nil
}

func TestStateStringAndTypedMismatch(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, countKey, 1)
	s := ib.Build()
	if !strings.Contains(s.String(), "count") {
		t.Fatalf("state string = %q, want count", s.String())
	}

	if got, ok := GetValue(s, NewKey[string]("count")); ok || got != "" {
		t.Fatalf("typed mismatch got %q, %v; want zero, false", got, ok)
	}

	var b Builder
	b.AddAll(map[string]any{"x": 1})
	if got := b.Build().Values()["x"]; got != 1 {
		t.Fatalf("AddAll into zero builder got %v, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// State unit behavior
// ---------------------------------------------------------------------------

func TestStateImmutability(t *testing.T) {
	ib := NewBuilder()
	SetValue(ib, countKey, 1)
	s := ib.Build()

	// Mutating the returned map must not affect the state.
	vals := s.Values()
	vals["count"] = 999
	if n, _ := GetValue(s, countKey); n != 1 {
		t.Fatalf("state mutated through Values(): count = %d, want 1", n)
	}

	// Builder reuse after Build must not affect the built state.
	SetValue(ib, countKey, 2)
	if n, _ := GetValue(s, countKey); n != 1 {
		t.Fatalf("state mutated through builder reuse: count = %d, want 1", n)
	}
}

func TestGetRequiredValueMissing(t *testing.T) {
	_, err := GetRequiredValue(NewState(), countKey)
	if err == nil {
		t.Fatal("expected error for missing required key")
	}
}

func TestZeroValueStateUsable(t *testing.T) {
	var s State // zero value
	if _, ok := GetValue(s, countKey); ok {
		t.Fatal("zero-value state should have no values")
	}
	b := s.ToBuilder()
	SetValue(b, countKey, 5)
	if n, _ := GetValue(b.Build(), countKey); n != 5 {
		t.Fatal("zero-value state should be buildable")
	}
}

// ---------------------------------------------------------------------------
// tiny helpers (avoid pulling strconv into the test for one call)
// ---------------------------------------------------------------------------

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

type atomicOnce struct{ done atomic.Bool }

func (o *atomicOnce) do(f func()) {
	if o.done.CompareAndSwap(false, true) {
		f()
	}
}
