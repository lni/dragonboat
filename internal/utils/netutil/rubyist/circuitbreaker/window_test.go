package circuit

import (
	"testing"
	"time"

	"github.com/facebookgo/clock"
)

func TestWindowCounts(t *testing.T) {
	w := newWindow(time.Millisecond*10, 2)
	w.Fail()
	w.Fail()
	w.Success()
	w.Success()

	if f := w.Failures(); f != 2 {
		t.Fatalf("expected window to have 2 failures, got %d", f)
	}

	if s := w.Successes(); s != 2 {
		t.Fatalf("expected window to have 2 successes, got %d", s)
	}

	if r := w.ErrorRate(); r != 0.5 {
		t.Fatalf("expected window to have 0.5 error rate, got %f", r)
	}

	w.Reset()
	if f := w.Failures(); f != 0 {
		t.Fatalf("expected reset window to have 0 failures, got %d", f)
	}
	if s := w.Successes(); s != 0 {
		t.Fatalf("expected window to have 0 successes, got %d", s)
	}
}

func TestWindowSlides(t *testing.T) {
	c := clock.NewMock()

	w := newWindow(time.Millisecond*10, 2)
	w.clock = c
	w.lastAccess = c.Now()

	w.Fail()
	c.Add(time.Millisecond * 6)
	w.Fail()

	counts := 0
	w.buckets.Do(func(x interface{}) {
		b := x.(*bucket)
		if b.failure > 0 {
			counts++
		}
	})

	if counts != 2 {
		t.Fatalf("expected 2 buckets to have failures, got %d", counts)
	}

	c.Add(time.Millisecond * 15)
	w.Success()
	counts = 0
	w.buckets.Do(func(x interface{}) {
		b := x.(*bucket)
		if b.failure > 0 {
			counts++
		}
	})

	if counts != 0 {
		t.Fatalf("expected 0 buckets to have failures, got %d", counts)
	}
}
