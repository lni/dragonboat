# Porcupine

Porcupine is a fast linearizability checker for testing the correctness of
distributed systems. It takes a sequential specification as executable Go code,
along with a concurrent history, and it determines whether the history is
linearizable with respect to the sequential specification.

Porcupine implements the algorithm described in [Faster linearizability
checking via P-compositionality][faster-linearizability-checking], an
optimization of the algorithm described in [Testing for
Linearizability][linearizability-testing].

Porcupine is faster and can handle more histories than [Knossos][knossos]'s
linearizability checker. Testing on the data in `test_data/jepsen/`, Porcupine
is generally **1,000x**-**10,000x** faster and has a much smaller memory
footprint. On histories where it can take advantage of P-compositionality,
Porcupine can be millions of times faster.

## Usage

Porcupine takes an executable model of a system along with a history, and it
runs a decision procedure to determine if the history is linearizable with
respect to the model. Porcupine supports specifying history in two ways, either
as a list of operations with given call and return times, or as a list of
call/return events in time order.

See [`model.go`](model.go) for documentation on how to write a model or specify
histories. Once you've written a model and have a history, you can use the
`CheckOperations` and `CheckEvents` functions to determine if your history is
linearizable.

## Example

Suppose we're testing linearizability for operations on a read/write register
that's initialized to `0`. We write a sequential specification for the register
like this:

```go
type registerInput struct {
    op bool // false = write, true = read
    value int
}

// a sequential specification of a register
registerModel := porcupine.Model{
    Init: func() interface{} {
        return 0
    },
    // step function: takes a state, input, and output, and returns whether it
    // was a legal operation, along with a new state
    Step: func(state, input, output interface{}) (bool, interface{}) {
        regInput := input.(registerInput)
        if regInput.op == false {
            return true, regInput.value // always ok to execute a write
        } else {
            readCorrectValue := output == state
            return readCorrectValue, state // state is unchanged
        }
    },
}
```

Suppose we have the following concurrent history from a set of 3 clients. In a
row, the first `|` is when the operation was invoked, and the second `|` is
when the operation returned.

```
C0:  |-------- Write(100) --------|
C1:      |--- Read(): 100 ---|
C2:          |- Read(): 0 -|
```

We encode this history as follows:

```go
events := []porcupine.Event{
    // C0: Write(100)
    {Kind: porcupine.CallEvent, Value: registerInput{false, 100}, Id: 0},
    // C1: Read()
    {Kind: porcupine.CallEvent, Value: registerInput{true, 0}, Id: 1},
    // C2: Read()
    {Kind: porcupine.CallEvent, Value: registerInput{true, 0}, Id: 2},
    // C2: Completed Read -> 0
    {Kind: porcupine.ReturnEvent, Value: 0, Id: 2},
    // C1: Completed Read -> 100
    {Kind: porcupine.ReturnEvent, Value: 100, Id: 1},
    // C0: Completed Write
    {Kind: porcupine.ReturnEvent, Value: 0, Id: 0},
}
```

We can have Porcupine check the linearizability of the history as follows:

```go
ok := porcupine.CheckEvents(registerModel, events)
// returns true
```

Now, suppose we have another history:

```
C0:  |------------- Write(200) -------------|
C1:    |- Read(): 200 -|
C2:                        |- Read(): 0 -|
```

We can check the history with Porcupine and see that it's not linearizable:

```go
ok := porcupine.CheckEvents(registerModel, events)
// returns false
```

See [`porcupine_test.go`](porcupine_test.go) for more examples on how to write
models and histories.

## Notes

Porcupine's API is not stable yet. Please vendor this package before using it.

## Citation

If you use Porcupine in any way in academic work, please cite the following:

```
@misc{athalye2017porcupine,
  author = {Anish Athalye},
  title = {Porcupine: A fast linearizability checker in {Go}},
  year = {2017},
  howpublished = {\url{https://github.com/anishathalye/porcupine}},
  note = {commit xxxxxxx}
}
```

## License

Copyright (c) 2017-2018 Anish Athalye. Released under the MIT License. See
[LICENSE.md][license] for details.

[faster-linearizability-checking]: https://arxiv.org/pdf/1504.00204.pdf
[linearizability-testing]: http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf
[knossos]: https://github.com/jepsen-io/knossos
[license]: LICENSE.md
