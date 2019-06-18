package porcupine

// Operation ...
type Operation struct {
	Input  interface{}
	Call   int64 // invocation time
	Output interface{}
	Return int64 // response time
}

// EventKind ...
type EventKind bool

const (
	// CallEvent ...
	CallEvent EventKind = false
	// ReturnEvent ...
	ReturnEvent EventKind = true
)

// Event ...
type Event struct {
	Kind  EventKind
	Value interface{}
	Id    uint
}

// Model ...
type Model struct {
	// Partition functions, such that a history is linearizable if an only
	// if each partition is linearizable. If you don't want to implement
	// this, you can always use the `NoPartition` functions implemented
	// below.
	Partition      func(history []Operation) [][]Operation
	PartitionEvent func(history []Event) [][]Event
	// Initial state of the system.
	Init func() interface{}
	// Step function for the system. Returns whether or not the system
	// could take this step with the given inputs and outputs and also
	// returns the new state. This should not mutate the existing state.
	Step func(state interface{}, input interface{}, output interface{}) (bool, interface{})
	// Equality on states. If you are using a simple data type for states,
	// you can use the `ShallowEqual` function implemented below.
	Equal func(state1, state2 interface{}) bool
}

// NoPartition ...
func NoPartition(history []Operation) [][]Operation {
	return [][]Operation{history}
}

// NoPartitionEvent ...
func NoPartitionEvent(history []Event) [][]Event {
	return [][]Event{history}
}

// ShallowEqual ...
func ShallowEqual(state1, state2 interface{}) bool {
	return state1 == state2
}
