package circuit

import (
	"fmt"

	"github.com/peterbourgon/g2s"

	"io/ioutil"
	"log"
	"time"
)

func ExampleNewThresholdBreaker() {
	// This example sets up a ThresholdBreaker that will trip if remoteCall returns
	// an error 10 times in a row. The error returned by Call() will be the error
	// returned by remoteCall, unless the breaker has been tripped, in which case
	// it will return ErrBreakerOpen.
	breaker := NewThresholdBreaker(10)
	err := breaker.Call(remoteCall, 0)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNewThresholdBreaker_manual() {
	// This example demonstrates the manual use of a ThresholdBreaker. The breaker
	// will trip when Fail is called 10 times in a row.
	breaker := NewThresholdBreaker(10)
	if breaker.Ready() {
		err := remoteCall()
		if err != nil {
			breaker.Fail()
			log.Fatal(err)
		} else {
			breaker.Success()
		}
	}
}

func ExampleNewThresholdBreaker_timeout() {
	// This example sets up a ThresholdBreaker that will trip if remoteCall
	// returns an error OR takes longer than one second 10 times in a row. The
	// error returned by Call() will be the error returned by remoteCall with
	// two exceptions: if remoteCall takes longer than one second the return
	// value will be ErrBreakerTimeout, if the breaker has been tripped the
	// return value will be ErrBreakerOpen.
	breaker := NewThresholdBreaker(10)
	err := breaker.Call(remoteCall, time.Second)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNewConsecutiveBreaker() {
	// This example sets up a FrequencyBreaker that will trip if remoteCall returns
	// an error 10 times in a row within a period of 2 minutes.
	breaker := NewConsecutiveBreaker(10)
	err := breaker.Call(remoteCall, 0)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleHTTPClient() {
	// This example sets up an HTTP client wrapped in a ThresholdBreaker. The
	// breaker will trip with the same behavior as ThresholdBreaker.
	client := NewHTTPClient(time.Second*5, 10, nil)

	resp, err := client.Get("http://example.com/resource.json")
	if err != nil {
		log.Fatal(err)
	}
	resource, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", resource)
}

func ExampleBreaker_events() {
	// This example demonstrates the BreakerTripped and BreakerReset callbacks. These are
	// available on all breaker types.
	breaker := NewThresholdBreaker(1)
	events := breaker.Subscribe()

	go func() {
		for {
			e := <-events
			switch e {
			case BreakerTripped:
				log.Println("breaker tripped")
			case BreakerReset:
				log.Println("breaker reset")
			case BreakerFail:
				log.Println("breaker fail")
			case BreakerReady:
				log.Println("breaker ready")
			}
		}
	}()

	breaker.Fail()
	breaker.Reset()
}

func ExamplePanel() {
	// This example demonstrates using a Panel to aggregate and manage circuit breakers.
	breaker1 := NewThresholdBreaker(10)
	breaker2 := NewRateBreaker(0.95, 100)

	panel := NewPanel()
	panel.Add("breaker1", breaker1)
	panel.Add("breaker2", breaker2)

	// Elsewhere in the code ...
	b1, _ := panel.Get("breaker1")
	b1.Call(func() error {
		// Do some work
		return nil
	}, 0)

	b2, _ := panel.Get("breaker2")
	b2.Call(func() error {
		// Do some work
		return nil
	}, 0)
}

func ExamplePanel_stats() {
	// This example demonstrates how to push circuit breaker stats to statsd via a Panel.
	// This example uses g2s. Anything conforming to the Statter interface can be used.
	s, err := g2s.Dial("udp", "statsd-server:8125")
	if err != nil {
		log.Fatal(err)
	}

	breaker := NewThresholdBreaker(10)
	panel := NewPanel()
	panel.Statter = s
	panel.StatsPrefixf = "sys.production.%s"
	panel.Add("x", breaker)

	breaker.Trip()  // sys.production.circuit.x.tripped
	breaker.Reset() // sys.production.circuit.x.reset, sys.production.circuit.x.trip-time
	breaker.Fail()  // sys.production.circuit.x.fail
	breaker.Ready() // sys.production.circuit.x.ready (if it's tripped and ready to retry)
}

func remoteCall() error {
	// Expensive remote call
	return nil
}
