package porcupine

import (
	"sort"
	"sync/atomic"
	"time"
)

type entryKind bool

const (
	callEntry   entryKind = false
	returnEntry           = true
)

type entry struct {
	kind  entryKind
	value interface{}
	id    uint
	time  int64
}

type byTime []entry

func (a byTime) Len() int {
	return len(a)
}

func (a byTime) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byTime) Less(i, j int) bool {
	return a[i].time < a[j].time
}

func makeEntries(history []Operation) []entry {
	var entries []entry = nil
	id := uint(0)
	for _, elem := range history {
		entries = append(entries, entry{
			callEntry, elem.Input, id, elem.Call})
		entries = append(entries, entry{
			returnEntry, elem.Output, id, elem.Return})
		id++
	}
	sort.Sort(byTime(entries))
	return entries
}

type node struct {
	value interface{}
	match *node // call if match is nil, otherwise return
	id    uint
	next  *node
	prev  *node
}

func insertBefore(n *node, mark *node) *node {
	if mark != nil {
		beforeMark := mark.prev
		mark.prev = n
		n.next = mark
		if beforeMark != nil {
			n.prev = beforeMark
			beforeMark.next = n
		}
	}
	return n
}

func length(n *node) uint {
	l := uint(0)
	for n != nil {
		n = n.next
		l++
	}
	return l
}

func renumber(events []Event) []Event {
	var e []Event
	m := make(map[uint]uint) // renumbering
	id := uint(0)
	for _, v := range events {
		if r, ok := m[v.Id]; ok {
			e = append(e, Event{v.Kind, v.Value, r})
		} else {
			e = append(e, Event{v.Kind, v.Value, id})
			m[v.Id] = id
			id++
		}
	}
	return e
}

func convertEntries(events []Event) []entry {
	var entries []entry
	for _, elem := range events {
		kind := callEntry
		if elem.Kind == ReturnEvent {
			kind = returnEntry
		}
		entries = append(entries, entry{kind, elem.Value, elem.Id, -1})
	}
	return entries
}

func makeLinkedEntries(entries []entry) *node {
	var root *node = nil
	match := make(map[uint]*node)
	for i := len(entries) - 1; i >= 0; i-- {
		elem := entries[i]
		if elem.kind == returnEntry {
			entry := &node{value: elem.value, match: nil, id: elem.id}
			match[elem.id] = entry
			insertBefore(entry, root)
			root = entry
		} else {
			entry := &node{value: elem.value, match: match[elem.id], id: elem.id}
			insertBefore(entry, root)
			root = entry
		}
	}
	return root
}

type cacheEntry struct {
	linearized bitset
	state      interface{}
}

func cacheContains(model Model, cache map[uint64][]cacheEntry, entry cacheEntry) bool {
	for _, elem := range cache[entry.linearized.hash()] {
		if entry.linearized.equals(elem.linearized) && model.Equal(entry.state, elem.state) {
			return true
		}
	}
	return false
}

type callsEntry struct {
	entry *node
	state interface{}
}

func lift(entry *node) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev
	match := entry.match
	match.prev.next = match.next
	if match.next != nil {
		match.next.prev = match.prev
	}
}

func unlift(entry *node) {
	match := entry.match
	match.prev.next = match
	if match.next != nil {
		match.next.prev = match
	}
	entry.prev.next = entry
	entry.next.prev = entry
}

func checkSingle(model Model, subhistory *node, kill *int32) bool {
	n := length(subhistory) / 2
	linearized := newBitset(n)
	cache := make(map[uint64][]cacheEntry) // map from hash to cache entry
	var calls []callsEntry

	state := model.Init()
	headEntry := insertBefore(&node{value: nil, match: nil, id: ^uint(0)}, subhistory)
	entry := subhistory
	for headEntry.next != nil {
		if atomic.LoadInt32(kill) != 0 {
			return false
		}
		if entry.match != nil {
			matching := entry.match // the return entry
			ok, newState := model.Step(state, entry.value, matching.value)
			if ok {
				newLinearized := linearized.clone().set(entry.id)
				newCacheEntry := cacheEntry{newLinearized, newState}
				if !cacheContains(model, cache, newCacheEntry) {
					hash := newLinearized.hash()
					cache[hash] = append(cache[hash], newCacheEntry)
					calls = append(calls, callsEntry{entry, state})
					state = newState
					linearized.set(entry.id)
					lift(entry)
					entry = headEntry.next
				} else {
					entry = entry.next
				}
			} else {
				entry = entry.next
			}
		} else {
			if len(calls) == 0 {
				return false
			}
			callsTop := calls[len(calls)-1]
			entry = callsTop.entry
			state = callsTop.state
			linearized.clear(entry.id)
			calls = calls[:len(calls)-1]
			unlift(entry)
			entry = entry.next
		}
	}
	return true
}

func fillDefault(model Model) Model {
	if model.Partition == nil {
		model.Partition = NoPartition
	}
	if model.PartitionEvent == nil {
		model.PartitionEvent = NoPartitionEvent
	}
	if model.Equal == nil {
		model.Equal = ShallowEqual
	}
	return model
}

func CheckOperations(model Model, history []Operation) bool {
	return CheckOperationsTimeout(model, history, 0)
}

// timeout = 0 means no timeout
// if this operation times out, then a false positive is possible
func CheckOperationsTimeout(model Model, history []Operation, timeout time.Duration) bool {
	model = fillDefault(model)
	partitions := model.Partition(history)
	ok := true
	results := make(chan bool)
	kill := int32(0)
	for _, subhistory := range partitions {
		l := makeLinkedEntries(makeEntries(subhistory))
		go func() {
			results <- checkSingle(model, l, &kill)
		}()
	}
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}
	count := 0
loop:
	for {
		select {
		case result := <-results:
			ok = ok && result
			if !ok {
				atomic.StoreInt32(&kill, 1)
				break loop
			}
			count++
			if count >= len(partitions) {
				break loop
			}
		case <-timeoutChan:
			break loop // if we time out, we might get a false positive
		}
	}
	return ok
}

func CheckEvents(model Model, history []Event) bool {
	return CheckEventsTimeout(model, history, 0)
}

// timeout = 0 means no timeout
// if this operation times out, then a false positive is possible
func CheckEventsTimeout(model Model, history []Event, timeout time.Duration) bool {
	model = fillDefault(model)
	partitions := model.PartitionEvent(history)
	ok := true
	results := make(chan bool)
	kill := int32(0)
	for _, subhistory := range partitions {
		l := makeLinkedEntries(convertEntries(renumber(subhistory)))
		go func() {
			results <- checkSingle(model, l, &kill)
		}()
	}
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}
	count := 0
loop:
	for {
		select {
		case result := <-results:
			ok = ok && result
			if !ok {
				atomic.StoreInt32(&kill, 1)
				break loop
			}
			count++
			if count >= len(partitions) {
				break loop
			}
		case <-timeoutChan:
			break loop // if we time out, we might get a false positive
		}
	}
	return ok
}
