package workflow_test

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"strings"

	"github.com/amityadav9314/aky-go-common/workflow"
)

// Declare typed keys once, usually as package-level vars.
var (
	exCount = workflow.NewKey[int]("count")
	exText  = workflow.NewKey[string]("text")
	exWord  = workflow.NewKey[string]("word")
	exUser  = workflow.NewKey[string]("user")
	exOrder = workflow.NewKey[[]string]("orders")
)

// --- Example 1: a simple linear pipeline -----------------------------------

type incrementTask struct{}

func (incrementTask) IsErrorFatal() bool { return true }

func (incrementTask) Run(_ context.Context, s workflow.State) (workflow.State, error) {
	n, _ := workflow.GetValue(s, exCount) // absent -> zero value
	b := s.ToBuilder()
	workflow.SetValue(b, exCount, n+1)
	return b.Build(), nil
}

func ExampleExecutionChain_linear() {
	chain := workflow.Define(workflow.NewState(), incrementTask{}).
		Next(incrementTask{}).
		Next(incrementTask{}).
		Next(incrementTask{})

	final, err := chain.Execute(context.Background())
	if err != nil {
		panic(err)
	}
	n, _ := workflow.GetValue(final, exCount)
	fmt.Println(n)
	// Output: 4
}

// --- Example 2: fan-out with a MultiValuedTask -----------------------------

type splitWordsTask struct{}

func (splitWordsTask) IsErrorFatal() bool { return true }

func (splitWordsTask) Stream(_ context.Context, s workflow.State) iter.Seq2[workflow.State, error] {
	return func(yield func(workflow.State, error) bool) {
		text, err := workflow.GetRequiredValue(s, exText)
		if err != nil {
			yield(workflow.State{}, err)
			return
		}
		for _, w := range strings.Fields(text) {
			b := s.ToBuilder()
			workflow.SetValue(b, exWord, w)
			if !yield(b.Build(), nil) { // honor early consumer exit
				return
			}
		}
	}
}

type uppercaseTask struct{}

func (uppercaseTask) IsErrorFatal() bool { return true }

func (uppercaseTask) Run(_ context.Context, s workflow.State) (workflow.State, error) {
	w, _ := workflow.GetValue(s, exWord)
	b := s.ToBuilder()
	workflow.SetValue(b, exWord, strings.ToUpper(w))
	return b.Build(), nil
}

func ExampleExecutionChain_fanOut() {
	ib := workflow.NewBuilder()
	workflow.SetValue(ib, exText, "hello world")

	chain := workflow.Define(ib.Build(), splitWordsTask{}).Next(uppercaseTask{})

	states, err := chain.Collect(context.Background())
	if err != nil {
		panic(err)
	}
	var words []string
	for _, s := range states {
		w, _ := workflow.GetValue(s, exWord)
		words = append(words, w)
	}
	sort.Strings(words)
	fmt.Println(words)
	// Output: [HELLO WORLD]
}

// --- Example 3: parallel fan-in with a zipper ------------------------------

type fetchUserTask struct{}

func (fetchUserTask) IsErrorFatal() bool { return true }

func (fetchUserTask) Run(_ context.Context, s workflow.State) (workflow.State, error) {
	b := s.ToBuilder()
	workflow.SetValue(b, exUser, "Alice")
	return b.Build(), nil
}

type fetchOrdersTask struct{}

func (fetchOrdersTask) IsErrorFatal() bool { return true }

func (fetchOrdersTask) Run(_ context.Context, s workflow.State) (workflow.State, error) {
	b := s.ToBuilder()
	workflow.SetValue(b, exOrder, []string{"A", "B"})
	return b.Build(), nil
}

func ExampleExecutionChain_parallelZipper() {
	chain := workflow.DefineWith(workflow.NewState(), workflow.MergeStates,
		fetchUserTask{}, fetchOrdersTask{})

	final, err := chain.Execute(context.Background())
	if err != nil {
		panic(err)
	}
	user, _ := workflow.GetValue(final, exUser)
	orders, _ := workflow.GetValue(final, exOrder)
	fmt.Println(user, orders)
	// Output: Alice [A B]
}
